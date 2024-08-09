// Copyright 2019 The Go Cloud Development Kit Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package awsdynamodb

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"

	dyn2Types "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	dyn "github.com/aws/aws-sdk-go/service/dynamodb"
	"gocloud.dev/docstore/driver"
	"gocloud.dev/internal/gcerr"
)

var nullValue = new(dyn.AttributeValue).SetNULL(true)

type codec struct {
	kind  reflect.Kind
	value any
}

func (e *codec) EncodeNil() {
	e.encodeValue(nil, reflect.Interface) // using as a flag only
}
func (e *codec) EncodeBool(x bool) {
	e.encodeValue(x, reflect.Bool)
}
func (e *codec) EncodeInt(x int64) {
	e.encodeValue(x, reflect.Int64)
}
func (e *codec) EncodeUint(x uint64) {
	e.encodeValue(x, reflect.Uint64)
}
func (e *codec) EncodeBytes(x []byte) {
	e.encodeValue(x, reflect.Array) // using as a flag only
}
func (e *codec) EncodeFloat(x float64) {
	e.encodeValue(x, reflect.Float64)
}
func (e *codec) encodeValue(x any, kind reflect.Kind) {
	e.kind = kind
	e.value = x
}

func (e *codec) ListIndex(int) { panic("impossible") }
func (e *codec) MapKey(string) { panic("impossible") }

func (e *codec) EncodeString(x string) {
	if len(x) == 0 {
		e.EncodeNil()
	} else {
		e.encodeValue(x, reflect.String)
	}
}

func (e *codec) EncodeComplex(x complex128) {
	e.encodeValue(x, reflect.Complex128)
}

func (e *codec) EncodeList(n int) driver.Encoder {
	s := make([]*codec, n)
	e.kind = reflect.Slice
	e.value = s
	return &listEncoder{s: s}
}

func (e *codec) EncodeMap(n int) driver.Encoder {
	m := make(map[string]*codec, n)
	e.kind = reflect.Map
	e.value = m
	return &mapEncoder{m: m}
}

var typeOfGoTime = reflect.TypeOf(time.Time{})

// EncodeSpecial encodes time.Time specially.
func (e *codec) EncodeSpecial(v reflect.Value) (bool, error) {
	switch v.Type() {
	case typeOfGoTime:
		ts := v.Interface().(time.Time).Format(time.RFC3339Nano)
		e.EncodeString(ts)
	default:
		return false, nil
	}
	return true, nil
}

type listEncoder struct {
	s []*codec
	codec
}

func (e *listEncoder) ListIndex(i int) {
	ie := e.codec
	e.s[i] = &ie
	e.codec = codec{}
}

type mapEncoder struct {
	m map[string]*codec
	codec
}

func (e *mapEncoder) MapKey(k string) {
	ie := e.codec
	e.m[k] = &ie
	e.codec = codec{}
}

func encodeDoc(doc driver.Document) (*dyn.AttributeValue, error) {
	var e codec
	if err := doc.Encode(&e); err != nil {
		return nil, err
	}
	return e.asV1AttributeValue()
}

func (e *codec) asV1AttributeValue() (*dyn.AttributeValue, error) {
	var err error
	switch e.kind {
	case reflect.Interface: // flag for null
		return nullValue, nil
	case reflect.Bool:
		return new(dyn.AttributeValue).SetBOOL(e.value.(bool)), nil
	case reflect.Int64:
		return new(dyn.AttributeValue).SetN(strconv.FormatInt(e.value.(int64), 10)), nil
	case reflect.Uint64:
		return new(dyn.AttributeValue).SetN(strconv.FormatUint(e.value.(uint64), 10)), nil
	case reflect.Array: // flag for byte array
		return new(dyn.AttributeValue).SetB(e.value.([]byte)), nil
	case reflect.Float64:
		return new(dyn.AttributeValue).SetN(strconv.FormatFloat(e.value.(float64), 'f', -1, 64)), nil
	case reflect.String:
		return new(dyn.AttributeValue).SetS(e.value.(string)), nil
	case reflect.Complex128:
		return new(dyn.AttributeValue).SetL([]*dyn.AttributeValue{
			new(dyn.AttributeValue).SetN(strconv.FormatFloat(real(e.value.(complex128)), 'f', -1, 64)),
			new(dyn.AttributeValue).SetN(strconv.FormatFloat(imag(e.value.(complex128)), 'f', -1, 64)),
		}), nil
	case reflect.Slice:
		es := e.value.([]*codec)
		s := make([]*dyn.AttributeValue, len(es))
		for i, se := range es {
			s[i], err = se.asV1AttributeValue()
			if err != nil {
				return nil, err
			}
		}
		return new(dyn.AttributeValue).SetL(s), nil
	case reflect.Map:
		em := e.value.(map[string]*codec)
		m := make(map[string]*dyn.AttributeValue, len(em))
		for k, se := range em {
			m[k], err = se.asV1AttributeValue()
			if err != nil {
				return nil, err
			}
		}
		return new(dyn.AttributeValue).SetM(m), nil
	}

	return nil, gcerr.Newf(gcerr.InvalidArgument, nil, "unknown type to encode %s", reflect.TypeOf(e.value).Kind())
}

func (e *codec) asV2AttributeValue() (dyn2Types.AttributeValue, error) {
	var err error
	switch e.kind {
	case reflect.Interface: // flag for null
		return &dyn2Types.AttributeValueMemberNULL{}, nil
	case reflect.Bool:
		return &dyn2Types.AttributeValueMemberBOOL{Value: e.value.(bool)}, nil
	case reflect.Int64:
		return &dyn2Types.AttributeValueMemberN{Value: strconv.FormatInt(e.value.(int64), 10)}, nil
	case reflect.Uint64:
		return &dyn2Types.AttributeValueMemberN{Value: strconv.FormatUint(e.value.(uint64), 10)}, nil
	case reflect.Array: // flag for byte array
		return &dyn2Types.AttributeValueMemberB{Value: e.value.([]byte)}, nil
	case reflect.Float64:
		return &dyn2Types.AttributeValueMemberN{Value: strconv.FormatFloat(e.value.(float64), 'f', -1, 64)}, nil
	case reflect.String:
		return &dyn2Types.AttributeValueMemberS{Value: e.value.(string)}, nil
	case reflect.Complex128:
		return &dyn2Types.AttributeValueMemberL{Value: []dyn2Types.AttributeValue{
			&dyn2Types.AttributeValueMemberN{Value: strconv.FormatFloat(real(e.value.(complex128)), 'f', -1, 64)},
			&dyn2Types.AttributeValueMemberN{Value: strconv.FormatFloat(imag(e.value.(complex128)), 'f', -1, 64)},
		}}, nil
	case reflect.Slice:
		es := e.value.([]*codec)
		s := make([]dyn2Types.AttributeValue, len(es))
		for i, se := range es {
			s[i], err = se.asV2AttributeValue()
			if err != nil {
				return nil, err
			}
		}
		return &dyn2Types.AttributeValueMemberL{Value: s}, nil
	case reflect.Map:
		em := e.value.(map[string]*codec)
		m := make(map[string]dyn2Types.AttributeValue, len(em))
		for k, se := range em {
			m[k], err = se.asV2AttributeValue()
			if err != nil {
				return nil, err
			}
		}
		return &dyn2Types.AttributeValueMemberM{Value: m}, nil
	}

	return nil, gcerr.Newf(gcerr.InvalidArgument, nil, "unknown type to encode %s", reflect.TypeOf(e.value).Kind())
}

func (e *codec) asV1AttributeMap() (map[string]*dyn.AttributeValue, error) {
	if e.kind != reflect.Map {
		return nil, gcerr.Newf(gcerr.InvalidArgument, nil, "incorrect type to encode %s, not map", reflect.TypeOf(e.value).Kind())
	}
	av, err := e.asV1AttributeValue()
	if err != nil {
		return nil, err
	}
	return av.M, nil
}

func (e *codec) asV2AttributeMap() (map[string]dyn2Types.AttributeValue, error) {
	if e.kind != reflect.Map {
		return nil, gcerr.Newf(gcerr.InvalidArgument, nil, "incorrect type to encode %s, not map", reflect.TypeOf(e.value).Kind())
	}
	av, err := e.asV2AttributeValue()
	if err != nil {
		return nil, err
	}
	return av.(*dyn2Types.AttributeValueMemberM).Value, nil
}

// Encode the key fields of the given document into a map AttributeValue.
// pkey and skey are the names of the partition key field and the sort key field.
// pkey must always be non-empty, but skey may be empty if the collection has no sort key.
func encodeDocKeyFields(doc driver.Document, pkey, skey string) (map[string]*codec, error) {
	m := map[string]*codec{}

	set := func(fieldName string) error {
		fieldVal, err := doc.GetField(fieldName)
		if err != nil {
			return err
		}
		attrVal, err := encodeValue(fieldVal)
		if err != nil {
			return err
		}
		m[fieldName] = attrVal
		return nil
	}

	if err := set(pkey); err != nil {
		return nil, err
	}
	if skey != "" {
		if err := set(skey); err != nil {
			return nil, err
		}
	}
	return m, nil
}

func encodeValue(v interface{}) (*codec, error) {
	var e codec
	if err := driver.Encode(reflect.ValueOf(v), &e); err != nil {
		return nil, err
	}
	return &e, nil
}

// //////////////////////////////////////////////////////////////

func decodeDoc(item *dyn.AttributeValue, doc driver.Document) error {
	d, err := newV1Decoder(item)
	if err != nil {
		return err
	}
	return doc.Decode(d)
}
func decodeDocV2(item dyn2Types.AttributeValue, doc driver.Document) error {
	d, err := newV2Decoder(item)
	if err != nil {
		return err
	}
	return doc.Decode(d)
}

func (d codec) String() string {
	return fmt.Sprint(d.value)
}

func (d codec) AsBool() (bool, bool) {
	if d.kind != reflect.Bool {
		return false, false
	}
	return d.value.(bool), true
}

func (d codec) AsNull() bool {
	return d.kind == reflect.Interface
}

func (d codec) AsString() (string, bool) {
	if d.kind != reflect.String {
		return "", false
	}
	// Empty string is represented by NULL.
	if d.value == nil {
		return "", true
	}
	return d.value.(string), true
}

func (d codec) AsInt() (int64, bool) {
	var v int64
	switch d.kind {
	case reflect.Int64:
		if d.value != nil {
			v = d.value.(int64)
		}
	case reflect.Uint64:
		if d.value != nil {
			v = int64(d.value.(uint64))
		}
	default:
		return 0, false
	}
	return v, true
}

func (d codec) AsUint() (uint64, bool) {
	var v uint64
	switch d.kind {
	case reflect.Int64:
		if d.value != nil {
			v = uint64(d.value.(int64))
		}
	case reflect.Uint64:
		if d.value != nil {
			v = d.value.(uint64)
		}
	default:
		return 0, false
	}
	return v, true
}

func (d codec) AsFloat() (float64, bool) {
	var v float64
	switch d.kind {
	case reflect.Int64:
		if d.value != nil {
			v = float64(d.value.(int64))
		}
	case reflect.Uint64:
		if d.value != nil {
			v = float64(d.value.(uint64))
		}
	case reflect.Float64:
		if d.value != nil {
			v = d.value.(float64)
		}
	default:
		return 0, false
	}
	return v, true
}

func (d codec) AsComplex() (complex128, bool) {
	if d.kind == reflect.Complex128 {
		return d.value.(complex128), true
	}
	if d.kind == reflect.Slice {
		l := d.value.([]*codec)
		if len(l) != 2 {
			return 0, false
		}

		r, ok := l[0].AsFloat()
		if !ok {
			return 0, false
		}
		i, ok := l[1].AsFloat()
		if !ok {
			return 0, false
		}
		return complex(r, i), true
	}

	return 0, false
}

func (d codec) AsBytes() ([]byte, bool) {
	if d.kind != reflect.Array {
		return nil, false
	}
	return d.value.([]byte), true
}

func (d codec) ListLen() (int, bool) {
	if d.kind != reflect.Slice {
		return 0, false
	}
	return len(d.value.([]*codec)), true
}

func (d codec) DecodeList(f func(i int, vd driver.Decoder) bool) {
	if d.kind != reflect.Slice {
		return
	}

	s := d.value.([]*codec)

	for i, el := range s {
		if !f(i, el) {
			break
		}
	}
}

func (d codec) MapLen() (int, bool) {
	if d.kind != reflect.Map {
		return 0, false
	}
	return len(d.value.(map[string]*codec)), true
}

func (d codec) DecodeMap(f func(key string, vd driver.Decoder, exactMatch bool) bool) {
	if d.kind != reflect.Map {
		return
	}

	m := d.value.(map[string]*codec)

	for k, dec := range m {
		if !f(k, dec, true) {
			break
		}
	}
}

func (d codec) AsInterface() (interface{}, error) {
	return d.value, nil
}

func newV1Decoder(av *dyn.AttributeValue) (*codec, error) {
	switch {
	case av.NULL != nil:
		return newDecoder(nil, reflect.Interface), nil
	case av.BOOL != nil:
		return newDecoder(*av.BOOL, reflect.Bool), nil
	case av.N != nil:
		f, err := strconv.ParseFloat(*av.N, 64)
		if err != nil {
			return nil, err
		}
		i := int64(f)
		if float64(i) == f {
			return newDecoder(i, reflect.Int64), nil
		}
		u := uint64(f)
		if float64(u) == f {
			return newDecoder(u, reflect.Uint64), nil
		}
		return newDecoder(f, reflect.Float64), nil

	case av.B != nil:
		return newDecoder(av.B, reflect.Array), nil
	case av.S != nil:
		t, err := time.Parse(time.RFC3339Nano, *av.S)
		if err == nil {
			return newDecoder(t, reflect.Chan), nil
		}
		return newDecoder(*av.S, reflect.String), nil

	case av.L != nil:
		s := make([]*codec, len(av.L))
		for i, v := range av.L {
			x, err := newV1Decoder(v)
			if err != nil {
				return nil, err
			}
			s[i] = x
		}
		return newDecoder(s, reflect.Slice), nil

	case av.M != nil:
		m := make(map[string]*codec, len(av.M))
		for k, v := range av.M {
			x, err := newV1Decoder(v)
			if err != nil {
				return nil, err
			}
			m[k] = x
		}
		return newDecoder(m, reflect.Map), nil

	// recognized but unsupported types
	case av.SS != nil:
		return newDecoder(av.SS, reflect.Invalid), nil
	case av.NS != nil:
		return newDecoder(av.NS, reflect.Invalid), nil
	case av.BS != nil:
		return newDecoder(av.BS, reflect.Invalid), nil

	default:
		return nil, fmt.Errorf("awsdynamodb: AttributeValue %s not supported", av)
	}
}

func newV2Decoder(av dyn2Types.AttributeValue) (*codec, error) {
	var err error

	switch t := av.(type) {
	case *dyn2Types.AttributeValueMemberB:
		return newDecoder(t.Value, reflect.Array), nil
	case *dyn2Types.AttributeValueMemberBOOL:
		return newDecoder(t.Value, reflect.Bool), nil
	case *dyn2Types.AttributeValueMemberBS:
		return newDecoder(t.Value, reflect.Invalid), nil
	case *dyn2Types.AttributeValueMemberL:
		l := make([]*codec, len(t.Value))
		for i, v := range t.Value {
			l[i], err = newV2Decoder(v)
			if err != nil {
				return nil, err
			}
		}
		return newDecoder(l, reflect.Slice), nil
	case *dyn2Types.AttributeValueMemberM:
		m := make(map[string]*codec, len(t.Value))
		for k, v := range t.Value {
			m[k], err = newV2Decoder(v)
			if err != nil {
				return nil, err
			}
		}
		return newDecoder(m, reflect.Map), err
	case *dyn2Types.AttributeValueMemberN:
		f, err := strconv.ParseFloat(t.Value, 64)
		if err != nil {
			return nil, err
		}
		i := int64(f)
		if float64(i) == f {
			return newDecoder(i, reflect.Int64), nil
		}
		u := uint64(f)
		if float64(u) == f {
			return newDecoder(u, reflect.Uint64), nil
		}
		return newDecoder(f, reflect.Float64), nil
	case *dyn2Types.AttributeValueMemberNS:
		return newDecoder(t.Value, reflect.Invalid), nil
	case *dyn2Types.AttributeValueMemberNULL:
		return newDecoder(nil, reflect.Interface), nil
	case *dyn2Types.AttributeValueMemberS:
		return newDecoder(t.Value, reflect.String), nil
	case *dyn2Types.AttributeValueMemberSS:
		return newDecoder(t.Value, reflect.Invalid), nil

	default:
		return nil, fmt.Errorf("awsdynamodb: AttributeValue %s not supported", av)
	}
}

func (d codec) AsSpecial(v reflect.Value) (bool, interface{}, error) {
	if d.kind == reflect.Invalid {
		unsupportedTypes := `unsupported type, the docstore driver for DynamoDB does
		not decode DynamoDB set types, such as string set, number set and binary set`
		return true, nil, errors.New(unsupportedTypes)
	}

	switch v.Type() {
	case typeOfGoTime:
		if d.kind == reflect.Chan {
			return true, d.value.(time.Time), nil
		}
		return false, nil, errors.New("expected string field for time.Time")
	}
	return false, nil, nil
}

func newDecoder(x any, kind reflect.Kind) *codec {
	return &codec{
		kind:  kind,
		value: x,
	}
}

func reReferenceMapString(m map[string]*string) map[string]string {
	mOut := make(map[string]string, len(m))

	for k, v := range m {
		if v != nil {
			mOut[k] = *v
		}
	}

	return mOut
}
