// weedo.go
package weedo

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

type Fid struct {
	Id, Key, Cookie uint64
}

type Client struct {
	master  *Master
	filers  map[string]*Filer
}

func NewClient(masterAddr string, filerUrls ...string) (*Client, error) {
	filers := make(map[string]*Filer)
	for _, url := range filerUrls {
		filer := NewFiler(url)
		filers[filer.Url] = filer
	}
    m, err := NewMaster(masterAddr)
    if err != nil {
        return nil, err
    }
	return &Client{
		master:  m,
		filers:  filers,
	}, nil
}

func (c *Client) Master() *Master {
	return c.master
}

func (c *Client) Volume(id, collection, dc string) (*Volume, error) {
    fid, err := ParseFid(id)
    if err != nil {
        return nil, err
    }

    volumeId := strconv.FormatUint(fid.Id, 10)
	vol, err := c.Master().Lookup(volumeId, collection, dc)
	if err != nil {
		return nil, err
	}
	return vol, nil
}

func (c *Client) Filer(url string) *Filer {
	filer := NewFiler(url)
	if v, ok := c.filers[filer.Url]; ok {
		return v
	}

	c.filers[filer.Url] = filer
	return filer
}

func ParseFid(s string) (fid Fid, err error) {
	a := strings.Split(s, ",")
	if len(a) != 2 || len(a[1]) <= 8 {
		return fid, errors.New("Fid format invalid")
	}
	if fid.Id, err = strconv.ParseUint(a[0], 10, 32); err != nil {
		return
	}
	index := len(a[1]) - 8
	if fid.Key, err = strconv.ParseUint(a[1][:index], 16, 64); err != nil {
		return
	}
	if fid.Cookie, err = strconv.ParseUint(a[1][index:], 16, 32); err != nil {
		return
	}

	return
}

func (c *Client) GetUrl(fid string, collection ...string) (publicUrl, url string, err error) {
	col := ""
	if len(collection) > 0 {
		col = collection[0]
	}
	vol, err := c.Volume(fid, col, "")
	if err != nil {
		return
	}

	publicUrl = fmt.Sprintf("%s/%s", vol.PublicUrl(), fid)
	url = fmt.Sprintf("%s/%s", vol.Url(), fid)
	return
}

/*
func (c *Client) GetUrls(fid string, collection ...string) (locations []Location, err error) {
	col := ""
	if len(collection) > 0 {
		col = collection[0]
	}
	vol, err := c.Volume(fid, col)
	if err != nil {
		return
	}
	for _, loc := range vol.Locations {
		loc.PublicUrl = fmt.Sprintf("%s/%s", loc.PublicUrl, fid)
		loc.Url = fmt.Sprintf("%s/%s", loc.Url, fid)
		locations = append(locations, loc)
	}
	return
}
*/

func (c *Client) AssignUpload(filename, mimeType string, file io.Reader) (fid string, size int64, err error) {
	return c.AssignUploadArgs(filename, mimeType, file, url.Values{})
}

func (c *Client) AssignUploadArgs(filename, mimeType string, file io.Reader, args url.Values) (fid string, size int64, err error) {
	fid, err = c.Master().AssignArgs(args)
	if err != nil {
		return
	}

	vol, err := c.Volume(fid, args.Get("collection"), "")
	if err != nil {
		return
	}
	size, err = vol.Upload(fid, 0, filename, mimeType, file)

	return
}

func (c *Client) Delete(fid string, count int, collection ...string) (err error) {
	col := ""
	if len(collection) > 0 {
		col = collection[0]
	}
	vol, err := c.Volume(fid, col, "")
	if err != nil {
		return
	}
	return vol.Delete(fid, count)
}

var quoteEscaper = strings.NewReplacer("\\", "\\\\", `"`, "\\\"")

func escapeQuotes(s string) string {
	return quoteEscaper.Replace(s)
}

func createFormFile(writer *multipart.Writer, fieldname, filename, mime string) (io.Writer, error) {
	h := make(textproto.MIMEHeader)
	h.Set("Content-Disposition",
		fmt.Sprintf(`form-data; name="%s"; filename="%s"`,
			escapeQuotes(fieldname), escapeQuotes(filename)))
	if len(mime) == 0 {
		mime = "application/octet-stream"
	}
	h.Set("Content-Type", mime)
	return writer.CreatePart(h)
}

func makeFormData(filename, mimeType string, content io.Reader) (formData io.Reader, contentType string, err error) {
	buf := new(bytes.Buffer)
	writer := multipart.NewWriter(buf)

	part, err := createFormFile(writer, "file", filename, mimeType)
	//log.Println(filename, mimeType)
	if err != nil {
		log.Println(err)
		return
	}
	_, err = io.Copy(part, content)
	if err != nil {
		log.Println(err)
		return
	}

	formData = buf
	contentType = writer.FormDataContentType()
	//log.Println(contentType)
	writer.Close()

	return
}

type uploadResp struct {
    Fid  string `json:"fid"`
    Url  string `json:"url"`
    Name string `json:"name"`
    Size int64  `json:"size"`
}

func upload(url string, contentType string, formData io.Reader) (r *uploadResp, err error) {
	resp, err := http.Post(url, contentType, formData)
	if err != nil {
		log.Println(err)
		return
	}
	defer resp.Body.Close()

	upload := new(uploadResp)
	if err = decodeJson(resp.Body, upload); err != nil {
		return
	}

	r = upload

	return
}

func del(url string) error {
	client := http.Client{}
	request, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(request)
	if err != nil {
		return err
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK &&
		resp.StatusCode != http.StatusAccepted {
		txt, _ := ioutil.ReadAll(resp.Body)
		return errors.New(string(txt))
	}
	return nil
}

func decodeJson(r io.Reader, v interface{}) error {
	return json.NewDecoder(r).Decode(v)
}
