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
	Bucket    string // GCS Bucket
	FileExt   string // Source file extension
	FilePath  string // Source file path
	ThumbExt  string // Thumbnail file extension
	ThumbPath string // Thumbnail file path
	Width     string // Thumbnail width
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

	// Extract GCS bucket, wiki ID, archOrTemp, filename, thumbname, and width.
	re := regexp.MustCompile("^/([0-9a-zA-Z-_.]+)/([0-9a-zA-Z-_.]+)/thumb/((?:archive|temp)/)?([^/]*)/(([0-9]+)px-.+)$")
	m := re.FindStringSubmatch(u.Path)
	// Bad thumb URI
	if m == nil {
		return ThumbParams{}, errors.New("Bad thumb URI")
	}

	// Extract source file extension.
	s := strings.Split(strings.ToLower(m[4]), ".")
	fileExt := ""
	if len(s) >= 2 {
		fileExt = s[len(s)-1]
	}

	// Extract thumbnail file extension.
	s = strings.Split(strings.ToLower(m[5]), ".")
	thumbExt := ""
	if len(s) >= 2 {
		thumbExt = s[len(s)-1]
	}

	return ThumbParams{
		Bucket:    m[1],
		FileExt:   fileExt,
		FilePath:  m[2] + "/" + m[3] + m[4],
		ThumbExt:  thumbExt,
		ThumbPath: m[2] + "/thumb/" + m[3] + m[4] + "/" + m[5],
		Width:     m[6],
	}, nil
}

func paramValidate(params ThumbParams) (error) {
	// Filter source file extension. MediaWiki does the MIME checking on upload, so this should be safe.
	if params.FileExt == "" || !slices.Contains([]string{"png", "gif", "jpg", "jpeg", "svg", "webp"}, params.FileExt) {
		return errors.New("Unsupported source file extension")
	}

	switch params.ThumbExt {
		case "":
			// Quick nil check.
		case "svg":
			// SVGs are only rasterised as PNGs.
			if params.ThumbExt == "png" {
				return nil
			}
		default:
			// Source file extension and thumbnail file extension are expected to match except for SVG rasterisation. JPEG and JPG aren't expected to be mixed.
			if params.ThumbExt == params.FileExt {
				return nil
			}
	}

	return errors.New("Unsupported thumbnail file extension")
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
	srcObj := client.Bucket(params.Bucket).Object(params.FilePath)
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

	// Perform thumbnailing with VIPS.
	inOpts := ""
	options := "strip,"
	switch params.FileExt {
		case "gif":
			// For handling animated GIF.
			inOpts = "[n=-1]"
		case "jpeg":
			fallthrough
		case "jpg":
			options += "Q=80"
		case "png":
			// For handling APNG.
			//inOpts = "[n=-1]"
		case "svg":
			// No additional options.
		case "webp":
			// For handling animated WEBP.
			inOpts = "[n=-1]"
			options += "lossless"
	}

	cmd := exec.Command("vipsthumbnail","--output=." + params.ThumbExt + "[" + options + "]","--size=" + params.Width + "x","--vips-concurrency=1","stdin" + inOpts)
	log.Println(cmd.Args)
	cmd.Stdin = bytes.NewBuffer(data)
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	if err != nil {
		log.Println(out)
		return nil, &ThumbError{"Command", err}
	}

	// Upload thumbnail to GCS.
	thumbObj := client.Bucket(params.Bucket).Object(params.ThumbPath)
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

	err = paramValidate(params)
	// Unsupported source or thumbnail file extensions.
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		log.Printf("paramValidate: %w", err)
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