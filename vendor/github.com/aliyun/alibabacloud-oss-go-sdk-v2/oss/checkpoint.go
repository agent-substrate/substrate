package oss

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
)

// ----- download checkpoint  -----
type downloadCheckpoint struct {
	CpDirPath  string // checkpoint dir full path
	CpFilePath string // checkpoint file full path
	VerifyData bool   // verify downloaded data in FilePath
	Loaded     bool   // If Info.Data.DownloadInfo is loaded from checkpoint

	Info struct { //checkpoint data
		Magic string // Magic
		MD5   string // The Data's MD5
		Data  struct {
			// source
			ObjectInfo struct {
				Name      string // oss://bucket/key
				VersionId string
				Range     string
			}
			ObjectMeta struct {
				Size         int64
				LastModified string
				ETag         string
			}

			// destination
			FilePath string // Local file

			// download info
			PartSize int64

			DownloadInfo struct {
				Offset int64
				CRC64  uint64
			}
		}
	}
}

func newDownloadCheckpoint(request *GetObjectRequest, filePath string, baseDir string, header http.Header, partSize int64) *downloadCheckpoint {
	var buf strings.Builder
	name := fmt.Sprintf("%v/%v", ToString(request.Bucket), ToString(request.Key))
	buf.WriteString("oss://" + escapePath(name, false))
	buf.WriteString("\n")
	buf.WriteString(ToString(request.VersionId))
	buf.WriteString("\n")
	buf.WriteString(ToString(request.Range))

	hashmd5 := md5.New()
	hashmd5.Write([]byte(buf.String()))
	srcHash := hex.EncodeToString(hashmd5.Sum(nil))

	absPath, _ := filepath.Abs(filePath)
	hashmd5.Reset()
	hashmd5.Write([]byte(absPath))
	destHash := hex.EncodeToString(hashmd5.Sum(nil))

	var dir string
	if baseDir == "" {
		dir = os.TempDir()
	} else {
		dir = filepath.Dir(baseDir)
	}

	cpFilePath := filepath.Join(dir, fmt.Sprintf("%v-%v%v", srcHash, destHash, CheckpointFileSuffixDownloader))

	cp := &downloadCheckpoint{
		CpFilePath: cpFilePath,
		CpDirPath:  dir,
	}

	objectSize, _ := strconv.ParseInt(header.Get("Content-Length"), 10, 64)

	cp.Info.Magic = CheckpointMagic
	cp.Info.Data.ObjectInfo.Name = "oss://" + name
	cp.Info.Data.ObjectInfo.VersionId = ToString(request.VersionId)
	cp.Info.Data.ObjectInfo.Range = ToString(request.Range)
	cp.Info.Data.ObjectMeta.Size = objectSize
	cp.Info.Data.ObjectMeta.LastModified = header.Get("Last-Modified")
	cp.Info.Data.ObjectMeta.ETag = header.Get("ETag")
	cp.Info.Data.FilePath = filePath
	cp.Info.Data.PartSize = partSize

	return cp
}

// load checkpoint from local file
func (cp *downloadCheckpoint) load() error {
	if !DirExists(cp.CpDirPath) {
		return fmt.Errorf("Invaid checkpoint dir, %v", cp.CpDirPath)
	}

	if !FileExists(cp.CpFilePath) {
		return nil
	}

	if !cp.valid() {
		cp.remove()
		return nil
	}

	cp.Loaded = true

	return nil
}

func (cp *downloadCheckpoint) valid() bool {
	// Compare the CP's Magic and the MD5
	contents, err := os.ReadFile(cp.CpFilePath)
	if err != nil {
		return false
	}

	dcp := downloadCheckpoint{}

	if err = json.Unmarshal(contents, &dcp.Info); err != nil {
		return false
	}

	js, _ := json.Marshal(dcp.Info.Data)
	sum := md5.Sum(js)
	md5sum := hex.EncodeToString(sum[:])

	if CheckpointMagic != dcp.Info.Magic ||
		md5sum != dcp.Info.MD5 {
		return false
	}

	// compare
	if !reflect.DeepEqual(cp.Info.Data.ObjectInfo, dcp.Info.Data.ObjectInfo) ||
		!reflect.DeepEqual(cp.Info.Data.ObjectMeta, dcp.Info.Data.ObjectMeta) ||
		cp.Info.Data.FilePath != dcp.Info.Data.FilePath ||
		cp.Info.Data.PartSize != dcp.Info.Data.PartSize {
		return false
	}

	// download info
	if dcp.Info.Data.DownloadInfo.Offset < 0 {
		return false
	}

	if dcp.Info.Data.DownloadInfo.Offset == 0 &&
		dcp.Info.Data.DownloadInfo.CRC64 != 0 {
		return false
	}

	rOffset := int64(0)
	if len(cp.Info.Data.ObjectInfo.Range) > 0 {
		if r, err := ParseRange(cp.Info.Data.ObjectInfo.Range); err != nil {
			return false
		} else {
			rOffset = r.Offset
		}
	}

	if dcp.Info.Data.DownloadInfo.Offset < rOffset {
		return false
	}

	remains := (dcp.Info.Data.DownloadInfo.Offset - rOffset) % dcp.Info.Data.PartSize
	if remains != 0 {
		return false
	}

	//valid data
	if cp.VerifyData && dcp.Info.Data.DownloadInfo.CRC64 != 0 {
		if file, err := os.Open(cp.Info.Data.FilePath); err == nil {
			hash := NewCRC64(0)
			limitN := dcp.Info.Data.DownloadInfo.Offset - rOffset
			io.Copy(hash, io.LimitReader(file, limitN))
			file.Close()
			if hash.Sum64() != dcp.Info.Data.DownloadInfo.CRC64 {
				return false
			}
		}
	}

	// update
	cp.Info.Data.DownloadInfo = dcp.Info.Data.DownloadInfo

	return true
}

// dump dumps to file
func (cp *downloadCheckpoint) dump() error {
	// Calculate MD5
	js, _ := json.Marshal(cp.Info.Data)
	sum := md5.Sum(js)
	md5sum := hex.EncodeToString(sum[:])
	cp.Info.MD5 = md5sum

	// Serialize
	js, err := json.Marshal(cp.Info)
	if err != nil {
		return err
	}

	// Dump
	return os.WriteFile(cp.CpFilePath, js, FilePermMode)
}

func (cp *downloadCheckpoint) remove() error {
	return os.Remove(cp.CpFilePath)
}

// ----- upload chcekpoint  -----
type uploadCheckpoint struct {
	CpDirPath  string // checkpoint dir full path
	CpFilePath string // checkpoint file full path
	Loaded     bool   // If Info.Data.UploadInfo is loaded from checkpoint

	Info struct { //checkpoint data
		Magic string // Magic
		MD5   string // The Data's MD5
		Data  struct {
			// source
			FilePath string // Local file

			FileMeta struct {
				Size         int64
				LastModified string
			}

			// destination
			ObjectInfo struct {
				Name string // oss://bucket/key
			}

			// upload info
			PartSize int64

			UploadInfo struct {
				UploadId string
			}
		}
	}
}

func newUploadCheckpoint(request *PutObjectRequest, filePath string, baseDir string, fileInfo os.FileInfo, partSize int64) *uploadCheckpoint {
	name := fmt.Sprintf("%v/%v", ToString(request.Bucket), ToString(request.Key))
	hashmd5 := md5.New()
	hashmd5.Write([]byte("oss://" + escapePath(name, false)))
	destHash := hex.EncodeToString(hashmd5.Sum(nil))

	absPath, _ := filepath.Abs(filePath)
	hashmd5.Reset()
	hashmd5.Write([]byte(absPath))
	srcHash := hex.EncodeToString(hashmd5.Sum(nil))

	var dir string
	if baseDir == "" {
		dir = os.TempDir()
	} else {
		dir = filepath.Dir(baseDir)
	}

	cpFilePath := filepath.Join(dir, fmt.Sprintf("%v-%v%v", srcHash, destHash, CheckpointFileSuffixUploader))

	cp := &uploadCheckpoint{
		CpFilePath: cpFilePath,
		CpDirPath:  dir,
	}

	cp.Info.Magic = CheckpointMagic
	cp.Info.Data.FilePath = filePath
	cp.Info.Data.FileMeta.Size = fileInfo.Size()
	cp.Info.Data.FileMeta.LastModified = fileInfo.ModTime().String()
	cp.Info.Data.ObjectInfo.Name = "oss://" + name
	cp.Info.Data.PartSize = partSize

	return cp
}

// load checkpoint from local file
func (cp *uploadCheckpoint) load() error {
	if !DirExists(cp.CpDirPath) {
		return fmt.Errorf("Invaid checkpoint dir, %v", cp.CpDirPath)
	}

	if !FileExists(cp.CpFilePath) {
		return nil
	}

	if !cp.valid() {
		cp.remove()
		return nil
	}

	cp.Loaded = true

	return nil
}

func (cp *uploadCheckpoint) valid() bool {
	// Compare the CP's Magic and the MD5
	contents, err := os.ReadFile(cp.CpFilePath)
	if err != nil {
		return false
	}

	dcp := uploadCheckpoint{}

	if err = json.Unmarshal(contents, &dcp.Info); err != nil {
		return false
	}

	js, _ := json.Marshal(dcp.Info.Data)
	sum := md5.Sum(js)
	md5sum := hex.EncodeToString(sum[:])

	if CheckpointMagic != dcp.Info.Magic ||
		md5sum != dcp.Info.MD5 {
		return false
	}

	// compare
	if !reflect.DeepEqual(cp.Info.Data.ObjectInfo, dcp.Info.Data.ObjectInfo) ||
		!reflect.DeepEqual(cp.Info.Data.FileMeta, dcp.Info.Data.FileMeta) ||
		cp.Info.Data.FilePath != dcp.Info.Data.FilePath ||
		cp.Info.Data.PartSize != dcp.Info.Data.PartSize {
		return false
	}

	// download info
	if len(dcp.Info.Data.UploadInfo.UploadId) == 0 {
		return false
	}

	// update
	cp.Info.Data.UploadInfo = dcp.Info.Data.UploadInfo

	return true
}

// dump dumps to file
func (cp *uploadCheckpoint) dump() error {
	// Calculate MD5
	js, _ := json.Marshal(cp.Info.Data)
	sum := md5.Sum(js)
	md5sum := hex.EncodeToString(sum[:])
	cp.Info.MD5 = md5sum

	// Serialize
	js, err := json.Marshal(cp.Info)
	if err != nil {
		return err
	}

	// Dump
	return os.WriteFile(cp.CpFilePath, js, FilePermMode)
}

func (cp *uploadCheckpoint) remove() error {
	return os.Remove(cp.CpFilePath)
}
