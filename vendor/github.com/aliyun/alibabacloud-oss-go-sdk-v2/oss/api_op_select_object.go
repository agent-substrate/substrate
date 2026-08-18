package oss

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/xml"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"strconv"
	"strings"
)

// FrameType
const (
	DataFrameType        = 8388609
	ContinuousFrameType  = 8388612
	EndFrameType         = 8388613
	MetaEndFrameCSVType  = 8388614
	MetaEndFrameJSONType = 8388615
)

type CreateSelectObjectMetaRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,uploadId,required"`

	// The name of the object.
	Key *string `input:"path,key,required"`

	MetaRequest any `input:"nop,meta-request,required"`

	RequestCommon
}

type JsonMetaRequest struct {
	InputSerialization *InputSerialization `xml:"InputSerialization"`
	OverwriteIfExists  *bool               `xml:"OverwriteIfExists"`
}

type CsvMetaRequest struct {
	InputSerialization *InputSerialization `xml:"InputSerialization"`
	OverwriteIfExists  *bool               `xml:"OverwriteIfExists"`
}

type InputSerialization struct {
	CSV             *InputSerializationCSV  `xml:"CSV"`
	JSON            *InputSerializationJSON `xml:"JSON"`
	CompressionType *string                 `xml:"CompressionType"`
}

type InputSerializationCSV struct {
	RecordDelimiter *string `xml:"RecordDelimiter"`
	FieldDelimiter  *string `xml:"FieldDelimiter"`
	QuoteCharacter  *string `xml:"QuoteCharacter"`
}

type InputSerializationJSON struct {
	JSONType *string `xml:"Type"`
}

type CreateSelectObjectMetaResult struct {
	TotalScanned int64
	MetaStatus   int
	SplitsCount  int32
	RowsCount    int64
	ColumnsCount int32
	ErrorMsg     string
	ResultCommon
}

type ReadFlagInfo struct {
	OpenLine            bool
	ConsumedBytesLength int32
	EnablePayloadCrc    bool
	OutputRawData       bool
}

// CreateSelectObjectMeta You can call the CreateSelectObjectMeta operation to obtain information about an object, such as the total number of rows and the number of splits.
func (c *Client) CreateSelectObjectMeta(ctx context.Context, request *CreateSelectObjectMetaRequest, optFns ...func(*Options)) (*CreateSelectObjectMetaResult, error) {
	var err error
	if request == nil {
		request = &CreateSelectObjectMetaRequest{}
	}
	input := &OperationInput{
		OpName: "CreateSelectObjectMeta",
		Method: "POST",
		Bucket: request.Bucket,
		Key:    request.Key,
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
	}
	if err = c.marshalInput(request, input, marshalMetaRequest, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}
	result := &CreateSelectObjectMetaResult{}
	if err = c.unmarshalOutput(result, output, unmarshalBodyCreateSelectObjectMeta); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}
	return result, err
}

func marshalMetaRequest(request any, input *OperationInput) error {
	var builder strings.Builder
	var process string
	switch r := request.(*CreateSelectObjectMetaRequest).MetaRequest.(type) {
	case *JsonMetaRequest:
		process = "json/meta"
		builder.WriteString("<JsonMetaRequest>")
		if r.InputSerialization != nil {
			bs, err := xml.Marshal(r.InputSerialization)
			if err != nil {
				return err
			}
			builder.WriteString(string(bs))
		}
		if r.OverwriteIfExists != nil {
			builder.WriteString("<OverwriteIfExists>")
			builder.WriteString(strconv.FormatBool(*r.OverwriteIfExists))
			builder.WriteString("</OverwriteIfExists>")
		}
		builder.WriteString("</JsonMetaRequest>")
	case *CsvMetaRequest:
		r.encodeBase64()
		process = "csv/meta"
		builder.WriteString("<CsvMetaRequest>")
		if r.InputSerialization != nil {
			bs, err := xml.Marshal(r.InputSerialization)
			if err != nil {
				return err
			}
			builder.WriteString(string(bs))
		}
		if r.OverwriteIfExists != nil {
			builder.WriteString("<OverwriteIfExists>")
			builder.WriteString(strconv.FormatBool(*r.OverwriteIfExists))
			builder.WriteString("</OverwriteIfExists>")
		}
		builder.WriteString("</CsvMetaRequest>")
	default:
		return NewErrParamInvalid("MetaRequest")
	}
	input.Body = strings.NewReader(builder.String())
	if input.Parameters == nil {
		input.Parameters = map[string]string{}
	}
	input.Parameters["x-oss-process"] = process
	return nil
}

func unmarshalBodyCreateSelectObjectMeta(result any, output *OperationOutput) error {
	var err error
	if output.Body != nil {
		defer output.Body.Close()
		readerWrapper := &ReaderWrapper{
			Body:                output.Body,
			WriterForCheckCrc32: crc32.NewIEEE(),
		}
		if _, err = io.ReadAll(readerWrapper); err != nil {
			return err
		}
		result.(*CreateSelectObjectMetaResult).TotalScanned = readerWrapper.TotalScanned
		result.(*CreateSelectObjectMetaResult).MetaStatus = int(readerWrapper.Status)
		result.(*CreateSelectObjectMetaResult).SplitsCount = readerWrapper.SplitsCount
		result.(*CreateSelectObjectMetaResult).RowsCount = readerWrapper.RowsCount
		result.(*CreateSelectObjectMetaResult).ColumnsCount = readerWrapper.ColumnsCount
		result.(*CreateSelectObjectMetaResult).ErrorMsg = readerWrapper.ErrorMsg
	}
	return err
}

type SelectObjectRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,uploadId,required"`

	// The name of the object.
	Key *string `input:"path,key,required"`

	SelectRequest *SelectRequest `input:"nop,SelectRequest,required"`

	RequestCommon
}

type SelectObjectResult struct {
	Body io.ReadCloser
	ResultCommon
}

type SelectRequest struct {
	Expression                *string                   `xml:"Expression"`
	InputSerializationSelect  InputSerializationSelect  `xml:"InputSerialization"`
	OutputSerializationSelect OutputSerializationSelect `xml:"OutputSerialization"`
	SelectOptions             *SelectOptions            `xml:"Options"`
}

type OutputSerializationSelect struct {
	CsvBodyOutput    *CSVSelectOutput  `xml:"CSV"`
	JsonBodyOutput   *JSONSelectOutput `xml:"JSON"`
	OutputRawData    *bool             `xml:"OutputRawData"`
	KeepAllColumns   *bool             `xml:"KeepAllColumns"`
	EnablePayloadCrc *bool             `xml:"EnablePayloadCrc"`
	OutputHeader     *bool             `xml:"OutputHeader"`
}
type CSVSelectOutput struct {
	RecordDelimiter *string `xml:"RecordDelimiter"`
	FieldDelimiter  *string `xml:"FieldDelimiter"`
}
type JSONSelectOutput struct {
	RecordDelimiter *string `xml:"RecordDelimiter"`
}

type SelectOptions struct {
	SkipPartialDataRecord    *bool `xml:"SkipPartialDataRecord"`
	MaxSkippedRecordsAllowed *int  `xml:"MaxSkippedRecordsAllowed"`
}

type InputSerializationSelect struct {
	CsvBodyInput    *CSVSelectInput  `xml:"CSV"`
	JsonBodyInput   *JSONSelectInput `xml:"JSON"`
	CompressionType *string          `xml:"CompressionType"`
}

type CSVSelectInput struct {
	FileHeaderInfo             *string `xml:"FileHeaderInfo"`
	RecordDelimiter            *string `xml:"RecordDelimiter"`
	FieldDelimiter             *string `xml:"FieldDelimiter"`
	QuoteCharacter             *string `xml:"QuoteCharacter"`
	CommentCharacter           *string `xml:"CommentCharacter"`
	Range                      *string `xml:"Range"`
	SplitRange                 *string
	AllowQuotedRecordDelimiter *bool `xml:"AllowQuotedRecordDelimiter"`
}

type JSONSelectInput struct {
	JSONType                *string `xml:"Type"`
	Range                   *string `xml:"Range"`
	ParseJSONNumberAsString *bool   `xml:"ParseJsonNumberAsString"`
	SplitRange              *string
}

// SelectObject Executes SQL statements to perform operations on an object and obtains the execution results.
func (c *Client) SelectObject(ctx context.Context, request *SelectObjectRequest, optFns ...func(*Options)) (*SelectObjectResult, error) {
	var err error
	if request == nil {
		request = &SelectObjectRequest{}
	}
	input := &OperationInput{
		OpName: "SelectObject",
		Method: "POST",
		Bucket: request.Bucket,
		Key:    request.Key,
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
	}
	if err = c.marshalInput(request, input, marshalSelectObjectRequest, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}
	result := &SelectObjectResult{}
	err = unmarshalResultSelectObject(request, result, output)
	if err != nil {
		return nil, err
	}
	if err = c.unmarshalOutput(result, output); err != nil {
		return nil, err
	}
	return result, err
}

func marshalSelectObjectRequest(request any, input *OperationInput) error {
	var process string
	if request.(*SelectObjectRequest).SelectRequest != nil {
		if request.(*SelectObjectRequest).SelectRequest.InputSerializationSelect.JsonBodyInput == nil {
			process = "csv/select"
		} else {
			process = "json/select"
		}
		request.(*SelectObjectRequest).SelectRequest.encodeBase64()
	}
	if input.Parameters == nil {
		input.Parameters = map[string]string{}
	}
	input.Parameters["x-oss-process"] = process
	bs, err := xml.Marshal(request.(*SelectObjectRequest).SelectRequest)
	if err != nil {
		return err
	}
	input.Body = strings.NewReader(string(bs))
	return err
}

func unmarshalResultSelectObject(request *SelectObjectRequest, result *SelectObjectResult, output *OperationOutput) error {
	var err error
	if output.Body != nil {
		readerWrapper := &ReaderWrapper{
			Body:                output.Body,
			WriterForCheckCrc32: crc32.NewIEEE(),
		}
		if request.SelectRequest.OutputSerializationSelect.EnablePayloadCrc != nil && *request.SelectRequest.OutputSerializationSelect.EnablePayloadCrc == true {
			readerWrapper.EnablePayloadCrc = true
		}
		readerWrapper.OutputRawData = strings.ToUpper(output.Headers.Get("x-oss-select-output-raw")) == "TRUE"
		result.Body = readerWrapper
	}
	return err
}

// The adapter class for Select object's response.
// The response consists of frames. Each frame has the following format:

// Type  |   Payload Length |  Header Checksum | Payload | Payload Checksum

// |<4-->|  <--4 bytes------><---4 bytes-------><-n/a-----><--4 bytes--------->
// And we have three kind of frames.
// Data Frame:
// Type:8388609
// Payload:   Offset    |    Data
//            <-8 bytes>

// Continuous Frame
// Type:8388612
// Payload: Offset  (8-bytes)

// End Frame
// Type:8388613
// Payload: Offset | total scanned bytes | http status code | error message
//     <-- 8bytes--><-----8 bytes--------><---4 bytes-------><---variabe--->

// SelectObjectResponse defines HTTP response from OSS SelectObject
//type SelectObjectResponse struct {
//	Body        io.ReadCloser
//	Frame       SelectObjectResult
//	ReadTimeOut uint
//	Finish      bool
//	ResultCommon
//}

// ReaderWrapper defines HTTP response from OSS SelectObject
type ReaderWrapper struct {
	Body                io.ReadCloser
	Version             byte
	FrameType           int32
	PayloadLength       int32
	HeaderCheckSum      uint32
	Offset              uint64
	Data                string
	ClientCRC32         uint32
	ServerCRC32         uint32
	WriterForCheckCrc32 hash.Hash32
	HTTPStatusCode      int32
	TotalScanned        int64
	Status              int32
	SplitsCount         int32
	RowsCount           int64
	ColumnsCount        int32
	ErrorMsg            string
	PayloadChecksum     uint32
	ReadFlagInfo
	Finish bool
}

func (rw *ReaderWrapper) Read(p []byte) (n int, err error) {
	n, err = rw.readFrames(p)
	return
}

// Close http response body
func (rw *ReaderWrapper) Close() error {
	return rw.Body.Close()
}

// readFrames is read Frame
func (rw *ReaderWrapper) readFrames(p []byte) (int, error) {
	var nn int
	var err error
	var checkValid bool
	if rw.OutputRawData == true {
		nn, err = rw.Body.Read(p)
		return nn, err
	}

	if rw.Finish {
		return 0, io.EOF
	}

	for {
		// if this Frame is Read, then not reading Header
		if rw.OpenLine != true {
			err = rw.analysisHeader()
			if err != nil {
				return nn, err
			}
		}

		if rw.FrameType == DataFrameType {
			n, err := rw.analysisData(p[nn:])
			if err != nil {
				return nn, err
			}
			nn += n

			// if this Frame is read all data, then empty the Frame to read it with next frame
			if rw.ConsumedBytesLength == rw.PayloadLength-8 {
				checkValid, err = rw.checkPayloadSum()
				if err != nil || !checkValid {
					return nn, fmt.Errorf("%s", err.Error())
				}
				rw.emptyFrame()
			}

			if nn == len(p) {
				return nn, nil
			}
		} else if rw.FrameType == ContinuousFrameType {
			checkValid, err = rw.checkPayloadSum()
			if err != nil || !checkValid {
				return nn, fmt.Errorf("%s", err.Error())
			}
			rw.OpenLine = false
		} else if rw.FrameType == EndFrameType {
			err = rw.analysisEndFrame()
			if err != nil {
				return nn, err
			}
			checkValid, err = rw.checkPayloadSum()
			if checkValid {
				rw.Finish = true
			}
			return nn, err
		} else if rw.FrameType == MetaEndFrameCSVType {
			err = rw.analysisMetaEndFrameCSV()
			if err != nil {
				return nn, err
			}
			checkValid, err = rw.checkPayloadSum()
			if checkValid {
				rw.Finish = true
			}
			return nn, err
		} else if rw.FrameType == MetaEndFrameJSONType {
			err = rw.analysisMetaEndFrameJSON()
			if err != nil {
				return nn, err
			}
			checkValid, err = rw.checkPayloadSum()
			if checkValid {
				rw.Finish = true
			}
			return nn, err
		}
	}
}

type chanReadIO struct {
	readLen int
	err     error
}

func (rw *ReaderWrapper) readLen(p []byte) (int, error) {
	r := rw.Body
	ch := make(chan chanReadIO, 1)
	defer close(ch)
	go func(p []byte) {
		var needReadLength int
		readChan := chanReadIO{}
		needReadLength = len(p)
		for {
			n, err := r.Read(p[readChan.readLen:needReadLength])
			readChan.readLen += n
			if err != nil {
				readChan.err = err
				ch <- readChan
				return
			}

			if readChan.readLen == needReadLength {
				break
			}
		}
		ch <- readChan
	}(p)

	select {
	case result := <-ch:
		return result.readLen, result.err
	}
}

// analysisHeader is reading selectObject response body's header
func (rw *ReaderWrapper) analysisHeader() error {
	headFrameByte := make([]byte, 20)
	_, err := rw.readLen(headFrameByte)
	if err != nil {
		return fmt.Errorf("read response frame header failure,err:%s", err.Error())
	}

	frameTypeByte := headFrameByte[0:4]
	rw.Version = frameTypeByte[0]
	frameTypeByte[0] = 0
	bytesToInt(frameTypeByte, &rw.FrameType)

	if rw.FrameType != DataFrameType && rw.FrameType != ContinuousFrameType &&
		rw.FrameType != EndFrameType && rw.FrameType != MetaEndFrameCSVType && rw.FrameType != MetaEndFrameJSONType {
		return fmt.Errorf("unexpected frame type: %d", rw.FrameType)
	}

	payloadLengthByte := headFrameByte[4:8]
	bytesToInt(payloadLengthByte, &rw.PayloadLength)
	headCheckSumByte := headFrameByte[8:12]
	bytesToInt(headCheckSumByte, &rw.HeaderCheckSum)
	byteOffset := headFrameByte[12:20]
	bytesToInt(byteOffset, &rw.Offset)
	rw.OpenLine = true
	err = rw.writerCheckCrc32(byteOffset)
	return err
}

// analysisData is reading the DataFrameType data of selectObject response body
func (rw *ReaderWrapper) analysisData(p []byte) (int, error) {
	var needReadLength int32
	lenP := int32(len(p))
	restByteLength := rw.PayloadLength - 8 - rw.ConsumedBytesLength
	if lenP <= restByteLength {
		needReadLength = lenP
	} else {
		needReadLength = restByteLength
	}
	n, err := rw.readLen(p[:needReadLength])
	if err != nil {
		return n, fmt.Errorf("read frame data error,%s", err.Error())
	}
	rw.ConsumedBytesLength += int32(n)
	err = rw.writerCheckCrc32(p[:n])
	return n, err
}

// analysisEndFrame is reading the EndFrameType data of selectObject response body
func (rw *ReaderWrapper) analysisEndFrame() error {
	payLoadBytes := make([]byte, rw.PayloadLength-8)
	_, err := rw.readLen(payLoadBytes)
	if err != nil {
		return fmt.Errorf("read end frame error:%s", err.Error())
	}
	bytesToInt(payLoadBytes[0:8], &rw.TotalScanned)
	bytesToInt(payLoadBytes[8:12], &rw.HTTPStatusCode)
	errMsgLength := rw.PayloadLength - 20
	rw.ErrorMsg = string(payLoadBytes[12 : errMsgLength+12])
	err = rw.writerCheckCrc32(payLoadBytes)
	return err
}

// analysisMetaEndFrameCSV is reading the MetaEndFrameCSVType data of selectObject response body
func (rw *ReaderWrapper) analysisMetaEndFrameCSV() error {
	payLoadBytes := make([]byte, rw.PayloadLength-8)
	_, err := rw.readLen(payLoadBytes)
	if err != nil {
		return fmt.Errorf("read meta end csv frame error:%s", err.Error())
	}

	bytesToInt(payLoadBytes[0:8], &rw.TotalScanned)
	bytesToInt(payLoadBytes[8:12], &rw.Status)
	bytesToInt(payLoadBytes[12:16], &rw.SplitsCount)
	bytesToInt(payLoadBytes[16:24], &rw.RowsCount)
	bytesToInt(payLoadBytes[24:28], &rw.ColumnsCount)
	errMsgLength := rw.PayloadLength - 36
	rw.ErrorMsg = string(payLoadBytes[28 : errMsgLength+28])
	err = rw.writerCheckCrc32(payLoadBytes)
	return err
}

// analysisMetaEndFrameJSON is reading the MetaEndFrameJSONType data of selectObject response body
func (rw *ReaderWrapper) analysisMetaEndFrameJSON() error {
	payLoadBytes := make([]byte, rw.PayloadLength-8)
	_, err := rw.readLen(payLoadBytes)
	if err != nil {
		return fmt.Errorf("read meta end json frame error:%s", err.Error())
	}

	bytesToInt(payLoadBytes[0:8], &rw.TotalScanned)
	bytesToInt(payLoadBytes[8:12], &rw.Status)
	bytesToInt(payLoadBytes[12:16], &rw.SplitsCount)
	bytesToInt(payLoadBytes[16:24], &rw.RowsCount)
	errMsgLength := rw.PayloadLength - 32
	rw.ErrorMsg = string(payLoadBytes[24 : errMsgLength+24])
	err = rw.writerCheckCrc32(payLoadBytes)
	return err
}

func (rw *ReaderWrapper) checkPayloadSum() (bool, error) {
	payLoadChecksumByte := make([]byte, 4)
	n, err := rw.readLen(payLoadChecksumByte)
	if n == 4 {
		bytesToInt(payLoadChecksumByte, &rw.PayloadChecksum)
		rw.ServerCRC32 = rw.PayloadChecksum
		rw.ClientCRC32 = rw.WriterForCheckCrc32.Sum32()
		if rw.EnablePayloadCrc == true && rw.ServerCRC32 != 0 && rw.ServerCRC32 != rw.ClientCRC32 {
			return false, fmt.Errorf("unexpected frame type: %d, client %d but server %d", rw.FrameType, rw.ClientCRC32, rw.ServerCRC32)
		}
		return true, err
	}
	return false, fmt.Errorf("read checksum error:%s", err.Error())
}

func (rw *ReaderWrapper) writerCheckCrc32(p []byte) (err error) {
	err = nil
	if rw.EnablePayloadCrc == true {
		_, err = rw.WriterForCheckCrc32.Write(p)
	}
	return err
}

// emptyFrame is emptying SelectObjectResponse Frame information
func (rw *ReaderWrapper) emptyFrame() {
	rw.WriterForCheckCrc32 = crc32.NewIEEE()

	rw.Finish = false
	rw.ConsumedBytesLength = 0
	rw.OpenLine = false
	rw.Version = byte(0)
	rw.FrameType = 0
	rw.PayloadLength = 0
	rw.HeaderCheckSum = 0
	rw.Offset = 0
	rw.Data = ""

	rw.TotalScanned = 0
	rw.Status = 0
	rw.SplitsCount = 0
	rw.RowsCount = 0
	rw.ColumnsCount = 0

	rw.ErrorMsg = ""

	rw.PayloadChecksum = 0
}

// bytesToInt byte's array trans to int
func bytesToInt(b []byte, ret interface{}) {
	binBuf := bytes.NewBuffer(b)
	binary.Read(binBuf, binary.BigEndian, ret)
}

// jsonEncodeBase64 encode base64 of the SelectObject api request params
func (selectReq *SelectRequest) jsonEncodeBase64() {
	if selectReq == nil {
		return
	}
	if selectReq.Expression != nil {
		*selectReq.Expression = base64.StdEncoding.EncodeToString([]byte(*selectReq.Expression))
	}
	if selectReq.OutputSerializationSelect.JsonBodyOutput == nil {
		return
	}
	if selectReq.OutputSerializationSelect.JsonBodyOutput.RecordDelimiter != nil {
		*selectReq.OutputSerializationSelect.JsonBodyOutput.RecordDelimiter =
			base64.StdEncoding.EncodeToString([]byte(*selectReq.OutputSerializationSelect.JsonBodyOutput.RecordDelimiter))
	}
	if selectReq.InputSerializationSelect.JsonBodyInput.Range != nil {
		*selectReq.InputSerializationSelect.JsonBodyInput.Range = "line-range=" + *selectReq.InputSerializationSelect.JsonBodyInput.Range
	}
	if selectReq.InputSerializationSelect.JsonBodyInput.SplitRange != nil && *selectReq.InputSerializationSelect.JsonBodyInput.SplitRange != "" {
		selectReq.InputSerializationSelect.JsonBodyInput.Range = Ptr("split-range=" + *selectReq.InputSerializationSelect.JsonBodyInput.SplitRange)
		selectReq.InputSerializationSelect.JsonBodyInput.SplitRange = nil
	}
}

// encodeBase64 encode base64 of the CreateSelectObjectMeta api request params
func (meta *CsvMetaRequest) encodeBase64() {
	if meta == nil || meta.InputSerialization == nil {
		return
	}
	if meta.InputSerialization.CSV.RecordDelimiter != nil {
		*meta.InputSerialization.CSV.RecordDelimiter =
			base64.StdEncoding.EncodeToString([]byte(*meta.InputSerialization.CSV.RecordDelimiter))
	}
	if meta.InputSerialization.CSV.FieldDelimiter != nil {
		*meta.InputSerialization.CSV.FieldDelimiter =
			base64.StdEncoding.EncodeToString([]byte(*meta.InputSerialization.CSV.FieldDelimiter))
	}

	if meta.InputSerialization.CSV.QuoteCharacter != nil {
		*meta.InputSerialization.CSV.QuoteCharacter =
			base64.StdEncoding.EncodeToString([]byte(*meta.InputSerialization.CSV.QuoteCharacter))
	}
}

func (selectReq *SelectRequest) encodeBase64() {
	if selectReq.InputSerializationSelect.JsonBodyInput == nil {
		selectReq.csvEncodeBase64()
	} else {
		selectReq.jsonEncodeBase64()
	}
}

// csvEncodeBase64 encode base64 of the SelectObject api request params
func (selectReq *SelectRequest) csvEncodeBase64() {
	if selectReq == nil {
		return
	}
	if selectReq.Expression != nil {
		*selectReq.Expression = base64.StdEncoding.EncodeToString([]byte(*selectReq.Expression))
	}
	if selectReq.InputSerializationSelect.CsvBodyInput == nil {
		return
	}
	if selectReq.InputSerializationSelect.CsvBodyInput.RecordDelimiter != nil {
		*selectReq.InputSerializationSelect.CsvBodyInput.RecordDelimiter =
			base64.StdEncoding.EncodeToString([]byte(*selectReq.InputSerializationSelect.CsvBodyInput.RecordDelimiter))
	}
	if selectReq.InputSerializationSelect.CsvBodyInput.FieldDelimiter != nil {
		*selectReq.InputSerializationSelect.CsvBodyInput.FieldDelimiter =
			base64.StdEncoding.EncodeToString([]byte(*selectReq.InputSerializationSelect.CsvBodyInput.FieldDelimiter))
	}
	if selectReq.InputSerializationSelect.CsvBodyInput.QuoteCharacter != nil {
		*selectReq.InputSerializationSelect.CsvBodyInput.QuoteCharacter =
			base64.StdEncoding.EncodeToString([]byte(*selectReq.InputSerializationSelect.CsvBodyInput.QuoteCharacter))
	}
	if selectReq.InputSerializationSelect.CsvBodyInput.CommentCharacter != nil {
		*selectReq.InputSerializationSelect.CsvBodyInput.CommentCharacter =
			base64.StdEncoding.EncodeToString([]byte(*selectReq.InputSerializationSelect.CsvBodyInput.CommentCharacter))
	}
	if selectReq.InputSerializationSelect.CsvBodyInput.Range != nil && *selectReq.InputSerializationSelect.CsvBodyInput.Range != "" {
		*selectReq.InputSerializationSelect.CsvBodyInput.Range = "line-range=" + *selectReq.InputSerializationSelect.CsvBodyInput.Range
	}
	if selectReq.InputSerializationSelect.CsvBodyInput.SplitRange != nil && *selectReq.InputSerializationSelect.CsvBodyInput.SplitRange != "" {
		selectReq.InputSerializationSelect.CsvBodyInput.Range = Ptr("split-range=" + *selectReq.InputSerializationSelect.CsvBodyInput.SplitRange)
		selectReq.InputSerializationSelect.CsvBodyInput.SplitRange = nil
	}
}
