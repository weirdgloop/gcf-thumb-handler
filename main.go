package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"strings"

	"cloud.google.com/go/storage"
	"golang.org/x/exp/slices"
)

type ThumbError struct {
	Ctx string // Error context
	Err error  // Error
}

func (e *ThumbError) Error() string {
	return e.Ctx + ": " + e.Err.Error()
}

func (e *ThumbError) IsNotFound() bool {
	return e.Ctx == "NotFound"
}

type ThumbParams struct {
	Bucket string // GCS Bucket
	Dest   string // Thumbnail destination
	Src    string // Source image path
	Type   string // Source image file type
	Width  string // Thumbnail width
}

func main() {
	http.HandleFunc("/", thumbHandler)
	// Determine port for HTTP service.
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	// Start HTTP server.
	log.Printf("Listening on port %s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal(err)
	}
}

func paramExtract(rawURL string) (ThumbParams, error) {
	u, err := url.ParseRequestURI(rawURL)
	// Unparsable URI
	if err != nil {
		return ThumbParams{}, errors.New("Unparsable URI")
	}

	// Extract GCS bucket, wiki ID, archOrTemp, filename, thumbname, and width
	re := regexp.MustCompile("^/([0-9a-zA-Z-_.]+)/([0-9a-zA-Z-_.]+)/thumb/((?:archive|temp)/)?([^/]*)/(([0-9]+)px-.+)$")
	m := re.FindStringSubmatch(u.Path)
	// Bad thumb URI
	if m == nil {
		return ThumbParams{}, errors.New("Bad thumb URI")
	}

	// Filter file types. MediaWiki does the MIME checking on upload, so this should be safe.
	s := strings.Split(strings.ToLower(m[4]), ".")
	if len(s) < 2 || !slices.Contains([]string{"png", "gif", "jpg", "jpeg", "svg", "webp"}, s[len(s)-1]) {
		return ThumbParams{}, errors.New("Unsupported file type")
	}

	return ThumbParams{
		Bucket: m[1],
		Dest:   m[2] + "/thumb/" + m[3] + m[4] + "/" + m[5],
		Src:    m[2] + "/" + m[3] + m[4],
		Type:   s[len(s)-1],
		Width:  m[6],
	}, nil
}

func generateThumb(params ThumbParams) ([]byte, error) {
	// Initialise GCS client.
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, &ThumbError{"NewClient", err}
	}
	defer client.Close()

	// Prepare to read source image.
	srcObj := client.Bucket(params.Bucket).Object(params.Src)
	rc, err := srcObj.NewReader(ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return nil, &ThumbError{"NotFound", err}
		} else {
			return nil, &ThumbError{"NewReader", err}
		}
	}
	defer rc.Close()

	// Retrieve source image metadata for copying to thumbnail.
	attrs, err := srcObj.Attrs(ctx)
	if err != nil {
		return nil, &ThumbError{"SourceAttrs", err}
	}
	metadata := attrs.Metadata

	// Read source image into memory.
	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, &ThumbError{"ReadAll", err}
	}

	// Perform thumbnailing with ImageMagick.
	inOpts := ""
	options := "strip,"
	outType := params.Type
	switch params.Type {
		case "gif":
			// For handling animated GIF
			inOpts = "[n=-1]"
		case "jpeg":
			fallthrough
		case "jpg":
			options += "Q=80"
		case "png":
			// For handling APNG
			//inOpts = "[n=-1]"
		case "svg":
			outType = "png"
		case "webp":
			// For handling animated WEBP
			inOpts = "[n=-1]"
			options += "lossless"
	}

	cmd := exec.Command("vipsthumbnail","--output=." + outType + "[" + options + "]","--size=" + params.Width + "x","--vips-concurrency=1","stdin" + inOpts)
	log.Println(cmd.Args)
	cmd.Stdin = bytes.NewBuffer(data)
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	if err != nil {
		log.Println(out)
		return nil, &ThumbError{"Command", err}
	}

	// Upload thumbnail to GCS.
	thumbObj := client.Bucket(params.Bucket).Object(params.Dest)
	wc := thumbObj.NewWriter(ctx)
	if _, err = io.Copy(wc, bytes.NewBuffer(out)); err != nil {
		return nil, &ThumbError{"Copy", err}
	}
	if err = wc.Close(); err != nil {
		return nil, &ThumbError{"Close", err}
	}

	// Retrieve thumbnail's GCS metadata.
	attrs, err = thumbObj.Attrs(ctx)
	if err != nil {
		return nil, &ThumbError{"ThumbAttrs", err}
	}

	// Update thumbnail's GCS metadata with the source image's metadata.
	objectAttrsToUpdate := storage.ObjectAttrsToUpdate{
		Metadata: metadata,
	}
	if _, err = thumbObj.Update(ctx, objectAttrsToUpdate); err != nil {
		return nil, &ThumbError{"UpdateAttrs", err}
	}

	// Also send the image to the client.
	return out, nil
}

func thumbHandler(w http.ResponseWriter, r *http.Request) {
	params, err := paramExtract(r.RequestURI)
	// Unusable request URI
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		log.Printf("paramExtract: %w", err)
		return
	}

	out, err := generateThumb(params)
	// Unable to generate thumbnail.
	if err != nil {
		if err.(*ThumbError).IsNotFound() {
			w.WriteHeader(http.StatusNotFound)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
		log.Println(err)
	}

	// Send image to client.
	w.Write(out)
}