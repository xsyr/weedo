// weed master
package weedo

import (
    "errors"
    "time"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
    "sync"
)

var ErrVolumeLocationNotFound = errors.New("Location not found.")

type SystemStatus struct {
	Topology Topology
	Version  string
}

type Topology struct {
	DataCenters []DataCenter
	Free        int
	Max         int
    Layouts     []Layout `json:"layouts"`
}

type DataCenter struct {
	Free  int
	Max   int
    Id    string
	Racks []Rack
}

type Rack struct {
	DataNodes []DataNode
	Free      int
	Max       int
    Id        string
}

type DataNode struct {
	Free      int
	Max       int
	PublicUrl string
	Url       string
	Volumes   int
}

type Layout struct {
    Collection  string `json:"collection"`
    Replication string `json:"replication"`
    TTL         string `json:"ttl"`
    Writables   []uint64 `json:"writables"`
}


type Master struct {
	Url string

    done chan struct{}
    mtx sync.Mutex
    volDcMap map[string]string
}

func NewMaster(addr string) (*Master, error) {
    m := &Master{
		Url: addr,

        done : make(chan struct{}),
	}
    go m.updateTopo()
    return m, nil
}

// Assign a file key
func (m *Master) Assign() (string, error) {
	return m.AssignArgs(url.Values{})
}

// Assign multi file keys
func (m *Master) AssignN(count int) (fid string, err error) {
	args := url.Values{}
	if count > 0 {
		args.Set("count", strconv.Itoa(count))
	}

	return m.AssignArgs(args)
}

type assignResp struct {
    Count     int    `json:"count"`
    Fid       string `json:"fid"`
    Url       string `json:"url"`
    PublicUrl string `json:"publicUrl"`
}

// v0.4 or later only
func (m *Master) AssignArgs(args url.Values) (fid string, err error) {
	u := url.URL{
		Scheme:   "http",
		Host:     m.Url,
		Path:     "/dir/assign",
		RawQuery: args.Encode(),
	}

	resp, err := http.Get(u.String())
	if err != nil {
		return
	}
	defer resp.Body.Close()

	assign := new(assignResp)
	if err = decodeJson(resp.Body, assign); err != nil {
		log.Println(err)
		return
	}

	fid = assign.Fid

	return
}

type lookupResp struct {
    Locations []Location `json:"locations"`
    VolumeId     string `json:"volumeId"`
    Error        string `json:"error"`
}

type Location struct {
    Url       string `json:"url"`
    PublicUrl string `json:"publicUrl"`
}

func (m *Master) lookupNearestNode(lr *lookupResp, dc string) Location {
    m.mtx.Lock()
    defer m.mtx.Unlock()

    for _, l := range lr.Locations {
        if m.volDcMap[l.Url] == dc {
            return l
        }
    }

    return lr.Locations[0]
}

// Lookup Volume
func (m *Master) Lookup(volumeId, collection, dc string) (*Volume, error) {
	args := url.Values{}
	args.Set("volumeId", volumeId)
	args.Set("collection", collection)

	u := url.URL{
		Scheme:   "http",
		Host:     m.Url,
		Path:     "/dir/lookup",
		RawQuery: args.Encode(),
	}
	resp, err := http.Get(u.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	lookup := new(lookupResp)
	if err = decodeJson(resp.Body, lookup); err != nil {
		log.Println(err)
		return nil, err
	}

    if len(lookup.Error) != 0 {
        return nil, errors.New(lookup.Error)
    }

    if len(lookup.Locations) == 0 {
        return nil, ErrVolumeLocationNotFound
    }

    return NewVolume(m.lookupNearestNode(lookup, dc)), nil
}

// Force Garbage Collection
func (m *Master) GC(threshold float64) error {
	args := url.Values{}
	args.Set("garbageThreshold", strconv.FormatFloat(threshold, 'f', -1, 64))
	u := url.URL{
		Scheme:   "http",
		Host:     m.Url,
		Path:     "/vol/vacuum",
		RawQuery: args.Encode(),
	}
	resp, err := http.Get(u.String())
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// TODO: handle result
	return nil
}

// Pre-Allocate Volumes
func (m *Master) Grow(count int, collection, replication, dataCenter string) error {
	args := url.Values{}
	if count > 0 {
		args.Set("count", strconv.Itoa(count))
	}
	args.Set("collection", collection)
	args.Set("replication", replication)
	args.Set("dataCenter", dataCenter)

	return m.GrowArgs(args)
}

// v0.4 or later only
func (m *Master) GrowArgs(args url.Values) error {
	u := url.URL{
		Scheme:   "http",
		Host:     m.Url,
		Path:     "/vol/grow",
		RawQuery: args.Encode(),
	}
	resp, err := http.Get(u.String())
	resp.Body.Close()

	return err
}

func (m *Master) Submit(filename, mimeType string, file io.Reader) (fid string, size int64, err error) {
	return m.SubmitArgs(filename, mimeType, file, url.Values{})
}

// Upload File Directly
func (m *Master) SubmitArgs(filename, mimeType string, file io.Reader, args url.Values) (fid string, size int64, err error) {
	data, contentType, err := makeFormData(filename, mimeType, file)
	if err != nil {
		return
	}

	u := url.URL{
		Scheme:   "http",
		Host:     m.Url,
		Path:     "/submit",
		RawQuery: args.Encode(),
	}

	resp, err := upload(u.String(), contentType, data)
	if err == nil {
		fid = resp.Fid
		size = resp.Size
	}

	return
}

// Check System Status
func (m *Master) Status() (*SystemStatus, error) {
	u := url.URL{
		Scheme: "http",
		Host:   m.Url,
		Path:   "/dir/status",
	}
	resp, err := http.Get(u.String())
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	status := new(SystemStatus)
	if err = decodeJson(resp.Body, status); err != nil {
		log.Println(err)
		return nil, err
	}

	return status, nil
}

func (m *Master) updateTopo() {
    timeout := time.Tick(1 * time.Second)

    LOOP:
    for {
        select {
        case <-m.done: break LOOP
        case <-timeout:
        }

        s, err := m.Status()
        if err != nil {
            log.Println(err)
            continue
        }

        volDcMap := make(map[string]string)
        for _, d := range s.Topology.DataCenters {
            for _, r := range d.Racks {
                for _, n := range r.DataNodes {
                    volDcMap[n.Url] = d.Id
                }
            }
        }

        m.mtx.Lock()
        m.volDcMap = volDcMap
        m.mtx.Unlock()
    }
    m.done<-struct{}{}
}

func (m *Master) close() {
    m.done<-struct{}{}
    <-m.done
}

