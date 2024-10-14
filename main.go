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

const (
	MEDIA_IMAGE = "image"
	MEDIA_UNKNOWN = "unknown"
	MEDIA_VIDEO = "video"
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
	MediaType string // Source file media type
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

	// Determine media type.
	mediaType := MEDIA_UNKNOWN
	if slices.Contains([]string{"png", "gif", "jpg", "jpeg", "webp"}, fileExt) {
		mediaType = MEDIA_IMAGE
	} else if slices.Contains([]string{"mp4", "ogg", "ogv", "webm"}, fileExt) {
		mediaType = MEDIA_VIDEO
	}

	return ThumbParams{
		Bucket:    m[1],
		FileExt:   fileExt,
		FilePath:  m[2] + "/" + m[3] + m[4],
		MediaType: mediaType,
		ThumbExt:  thumbExt,
		ThumbPath: m[2] + "/thumb/" + m[3] + m[4] + "/" + m[5],
		Width:     m[6],
	}, nil
}

func paramValidate(params ThumbParams) (error) {
	// Filter source file extension. MediaWiki does the MIME checking on upload, so this should be safe.
	if params.MediaType == MEDIA_UNKNOWN {
		return errors.New("Unsupported source file extension")
	}
	// Videos are only thumbnailed as JPGs.
	if params.MediaType == MEDIA_VIDEO && params.ThumbExt == "jpg" {
		return nil
	}
	// Source file extension and thumbnail file extension are expected to otherwise match. JPEG and JPG aren't expected to be mixed.
	if params.ThumbExt == params.FileExt {
		return nil
	}
	// All other thumbnailing situations are unsupported.
	return errors.New("Unsupported thumbnail file extension")
}

func generateThumbFromFile(params ThumbParams) ([]byte, error) {
	// Initialise GCS client.
	ctx := context.Background()
	client, err := storage.NewClient(ctx, storage.WithJSONReads())
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

	f, err := os.CreateTemp("", "original")
	if err != nil {
		return nil, &ThumbError{"CreateTemp", err}
	}
	defer os.Remove(f.Name())

	if _, err := io.Copy(f, rc); err != nil {
		return nil, &ThumbError{"Copy", err}
	}

	// Parameters are based on Wikimedia's thumbor video plugin.
	// https://github.com/wikimedia/operations-software-thumbor-plugins/blob/7fe573abee23729964889caf20b78349205f0f97/wikimedia_thumbor/loader/video/__init__.py#L156
	cmd := exec.Command(
		"ffmpeg",
		// Input file type.
		"-f", params.FileExt,
		// Pass temp file name to ffmpeg.
		"-i", f.Name(),
		// Extract 1 frame.
		"-vframes", "1",
		// Disable audio.
		"-an",
		// Output as thumbnail.
		"-f", "image2pipe",
		// Set output dimensions based on desired width.
		"-vf", "scale=" + params.Width + ":-1",
		// Increase output quality.
		"-qscale:v", "1", "-qmin", "1", "-qmax", "1",
		// Disable verbose output.
		"-nostats",
		"-loglevel", "fatal",
		// Use stdout as output file.
		"pipe:1",
	)

	log.Println(cmd.Args)
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	if err != nil {
		log.Println(out)
		return nil, &ThumbError{"Command", err}
	}

	// Upload thumbnail to GCS.
	thumbObj := client.Bucket(params.Bucket).Object(params.ThumbPath)
	wc := thumbObj.NewWriter(ctx)
	// Use the source image's metadata for the thumbnail's metadata.
	wc.ObjectAttrs.Metadata = metadata

	if _, err = io.Copy(wc, bytes.NewBuffer(out)); err != nil {
		return out, &ThumbError{"Copy", err}
	}
	if err = wc.Close(); err != nil {
		return out, &ThumbError{"Close", err}
	}

	// Close temp file.
	if err = f.Close(); err != nil {
		return out, &ThumbError{"CloseTemp", err}
	}

	// Send the image to the client.
	return out, nil
}

func generateThumbFromPipe(params ThumbParams) ([]byte, error) {
	// Initialise GCS client.
	ctx := context.Background()
	client, err := storage.NewClient(ctx, storage.WithJSONReads())
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

	// Determine handler.
	var cmd *exec.Cmd
	if params.MediaType == MEDIA_IMAGE {
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
				options += "Q=96"
			case "png":
				// For handling APNG.
				//inOpts = "[n=-1]"
			case "webp":
				// For handling animated WEBP.
				inOpts = "[n=-1]"
				options += "lossless"
		}

		cmd = exec.Command("vipsthumbnail","--output=." + params.ThumbExt + "[" + options + "]","--size=" + params.Width + "x","--vips-concurrency=1","stdin" + inOpts)
	} else if params.MediaType == MEDIA_VIDEO {
		// Perform thumbnailing with FFmpeg.
		fmt := params.FileExt
		// Handle format aliases as FFmpeg does not.
		if fmt == "ogv" {
			fmt = "ogg"
		}
		// Parameters are based on Wikimedia's thumbor video plugin.
		// https://github.com/wikimedia/operations-software-thumbor-plugins/blob/7fe573abee23729964889caf20b78349205f0f97/wikimedia_thumbor/loader/video/__init__.py#L156
		cmd = exec.Command(
			"ffmpeg",
			// Input file type.
			"-f", fmt,
			// Use stdin as input file.
			"-i", "pipe:",
			// Extract 1 frame.
			"-vframes", "1",
			// Disable audio.
			"-an",
			// Output as thumbnail.
			"-f", "image2pipe",
			// Set output dimensions based on desired width.
			"-vf", "scale=" + params.Width + ":-1",
			// Increase output quality.
			"-qscale:v", "1", "-qmin", "1", "-qmax", "1",
			// Disable verbose output.
			"-nostats",
			"-loglevel", "fatal",
			// Use stdout as output file.
			"pipe:1",
		)
	} else {
		// No handler to perform thumbnailing.
		return nil, &ThumbError{"NoHandler", err}
	}
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
	// Use the source image's metadata for the thumbnail's metadata.
	wc.ObjectAttrs.Metadata = metadata

	if _, err = io.Copy(wc, bytes.NewBuffer(out)); err != nil {
		return out, &ThumbError{"Copy", err}
	}
	if err = wc.Close(); err != nil {
		return out, &ThumbError{"Close", err}
	}

	// Send the image to the client.
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

	var out []byte
	if params.FileExt == "mp4" {
		out, err = generateThumbFromFile(params)
	} else {
		out, err = generateThumbFromPipe(params)
	}
	// Unable to generate thumbnail.
	if err != nil {
		if err.(*ThumbError).IsNotFound() {
			w.WriteHeader(http.StatusNotFound)
		} else if out == nil {
			w.WriteHeader(http.StatusInternalServerError)
		}
		log.Println(err)
	}

	// Send image to client.
	w.Write(out)
}