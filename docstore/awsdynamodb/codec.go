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

type encoder struct {
	kind  reflect.Kind
	value any
}

func (e *encoder) EncodeNil() {
	e.encodeValue(nil, reflect.Interface) // using as a flag only
}
func (e *encoder) EncodeBool(x bool) {
	e.encodeValue(x, reflect.Bool)
}
func (e *encoder) EncodeInt(x int64) {
	e.encodeValue(x, reflect.Int64)
}
func (e *encoder) EncodeUint(x uint64) {
	e.encodeValue(x, reflect.Uint64)
}
func (e *encoder) EncodeBytes(x []byte) {
	e.encodeValue(x, reflect.Array) // using as a flag only
}
func (e *encoder) EncodeFloat(x float64) {
	e.encodeValue(x, reflect.Float64)
}
func (e *encoder) encodeValue(x any, kind reflect.Kind) {
	e.kind = kind
	e.value = x
}

func (e *encoder) ListIndex(int) { panic("impossible") }
func (e *encoder) MapKey(string) { panic("impossible") }

func (e *encoder) EncodeString(x string) {
	if len(x) == 0 {
		e.EncodeNil()
	} else {
		e.encodeValue(x, reflect.String)
	}
}

func (e *encoder) EncodeComplex(x complex128) {
	e.encodeValue(x, reflect.Complex128)
}

func (e *encoder) EncodeList(n int) driver.Encoder {
	s := make([]*encoder, n)
	e.kind = reflect.Slice
	e.value = s
	return &listEncoder{s: s}
}

func (e *encoder) EncodeMap(n int) driver.Encoder {
	m := make(map[string]*encoder, n)
	e.kind = reflect.Map
	e.value = m
	return &mapEncoder{m: m}
}

var typeOfGoTime = reflect.TypeOf(time.Time{})

// EncodeSpecial encodes time.Time specially.
func (e *encoder) EncodeSpecial(v reflect.Value) (bool, error) {
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
	s []*encoder
	encoder
}

func (e *listEncoder) ListIndex(i int) {
	ie := e.encoder
	e.s[i] = &ie
	e.encoder = encoder{}
}

type mapEncoder struct {
	m map[string]*encoder
	encoder
}

func (e *mapEncoder) MapKey(k string) {
	ie := e.encoder
	e.m[k] = &ie
	e.encoder = encoder{}
}

func encodeDoc(doc driver.Document) (*dyn.AttributeValue, error) {
	var e encoder
	if err := doc.Encode(&e); err != nil {
		return nil, err
	}
	return e.asV1AttributeValue()
}

func (e *encoder) asV1AttributeValue() (*dyn.AttributeValue, error) {
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
		es := e.value.([]*encoder)
		s := make([]*dyn.AttributeValue, len(es))
		for i, se := range es {
			s[i], err = se.asV1AttributeValue()
			if err != nil {
				return nil, err
			}
		}
		return new(dyn.AttributeValue).SetL(s), nil
	case reflect.Map:
		em := e.value.(map[string]*encoder)
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

func (e *encoder) asV2AttributeValue() (dyn2Types.AttributeValue, error) {
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
		es := e.value.([]*encoder)
		s := make([]dyn2Types.AttributeValue, len(es))
		for i, se := range es {
			s[i], err = se.asV2AttributeValue()
			if err != nil {
				return nil, err
			}
		}
		return &dyn2Types.AttributeValueMemberL{Value: s}, nil
	case reflect.Map:
		em := e.value.(map[string]*encoder)
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

// Encode the key fields of the given document into a map AttributeValue.
// pkey and skey are the names of the partition key field and the sort key field.
// pkey must always be non-empty, but skey may be empty if the collection has no sort key.
func encodeDocKeyFields(doc driver.Document, pkey, skey string) (*dyn.AttributeValue, error) {
	m := map[string]*dyn.AttributeValue{}

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
	return new(dyn.AttributeValue).SetM(m), nil
}

func encodeValue(v interface{}) (*dyn.AttributeValue, error) {
	var e encoder
	if err := driver.Encode(reflect.ValueOf(v), &e); err != nil {
		return nil, err
	}
	return e.asV1AttributeValue()
}

func encodeValueV2(v interface{}) (dyn2Types.AttributeValue, error) {
	var e encoder
	if err := driver.Encode(reflect.ValueOf(v), &e); err != nil {
		return nil, err
	}
	return e.asV2AttributeValue()
}

////////////////////////////////////////////////////////////////

func decodeDoc(item *dyn.AttributeValue, doc driver.Document) error {
	return doc.Decode(decoder{av1: item})
}
func decodeDocV2(item dyn2Types.AttributeValue, doc driver.Document) error {
	return doc.Decode(decoder{av2: item})
}

type decoder struct {
	av1 *dyn.AttributeValue
	av2 dyn2Types.AttributeValue
}

func (d decoder) String() string {
	if d.av1 != nil {
		return d.av1.String()
	}
	return fmt.Sprint(d.av2)
}

func (d decoder) AsBool() (bool, bool) {
	if d.av1 != nil {
		if d.av1.BOOL == nil {
			return false, false
		}
		return *d.av1.BOOL, true
	}
	if d.av2 != nil {
		if b, ok := d.av2.(*dyn2Types.AttributeValueMemberBOOL); ok {
			return b.Value, true
		}
	}
	return false, false
}

func (d decoder) AsNull() bool {
	if d.av1 != nil {
		return d.av1.NULL != nil
	}
	if d.av2 != nil {
		if _, ok := d.av2.(*dyn2Types.AttributeValueMemberNULL); ok {
			return ok
		}
	}
	return false
}

func (d decoder) AsString() (string, bool) {
	// Empty string is represented by NULL.
	if d.av1 != nil {
		if d.av1.NULL != nil {
			return "", true
		}
		if d.av1.S == nil {
			return "", false
		}
		return *d.av1.S, true
	}
	if d.av2 != nil {
		if _, ok := d.av2.(*dyn2Types.AttributeValueMemberNULL); ok {
			return "", true
		}
		if s, ok := d.av2.(*dyn2Types.AttributeValueMemberS); ok {
			return s.Value, true
		}
	}
	return "", false
}

func (d decoder) AsInt() (int64, bool) {
	if d.av1 != nil {
		if d.av1.N == nil {
			return 0, false
		}
		i, err := strconv.ParseInt(*d.av1.N, 10, 64)
		if err != nil {
			return 0, false
		}
		return i, true
	}
	if d.av2 != nil {
		if n, ok := d.av2.(*dyn2Types.AttributeValueMemberN); ok {
			i, err := strconv.ParseInt(n.Value, 10, 64)
			if err != nil {
				return 0, false
			}
			return i, true
		}
	}
	return 0, false
}

func (d decoder) AsUint() (uint64, bool) {
	if d.av1 != nil {
		if d.av1.N == nil {
			return 0, false
		}
		u, err := strconv.ParseUint(*d.av1.N, 10, 64)
		if err != nil {
			return 0, false
		}
		return u, true
	}
	if d.av2 != nil {
		if n, ok := d.av2.(*dyn2Types.AttributeValueMemberN); ok {
			i, err := strconv.ParseUint(n.Value, 10, 64)
			if err != nil {
				return 0, false
			}
			return i, true
		}
	}
	return 0, false
}

func (d decoder) AsFloat() (float64, bool) {
	if d.av1 != nil {
		if d.av1.N == nil {
			return 0, false
		}
		f, err := strconv.ParseFloat(*d.av1.N, 64)
		if err != nil {
			return 0, false
		}
		return f, true
	}
	if d.av2 != nil {
		if n, ok := d.av2.(*dyn2Types.AttributeValueMemberN); ok {
			f, err := strconv.ParseFloat(n.Value, 64)
			if err != nil {
				return 0, false
			}
			return f, true
		}
	}
	return 0, false
}

func (d decoder) AsComplex() (complex128, bool) {
	if d.av1 != nil {
		if len(d.av1.L) != 2 {
			return 0, false
		}
		r, ok := decoder{av1: d.av1.L[0]}.AsFloat()
		if !ok {
			return 0, false
		}
		i, ok := decoder{av1: d.av1.L[1]}.AsFloat()
		if !ok {
			return 0, false
		}
		return complex(r, i), true
	}
	if d.av2 != nil {
		if l, ok := d.av2.(*dyn2Types.AttributeValueMemberL); ok {
			if len(l.Value) != 2 {
				return 0, false
			}
			r, ok := decoder{av2: l.Value[0]}.AsFloat()
			if !ok {
				return 0, false
			}
			i, ok := decoder{av2: l.Value[1]}.AsFloat()
			if !ok {
				return 0, false
			}
			return complex(r, i), true
		}
	}
	return 0, false
}

func (d decoder) AsBytes() ([]byte, bool) {
	if d.av1 != nil {
		if d.av1.B == nil {
			return nil, false
		}
		return d.av1.B, true
	}
	if d.av2 != nil {
		if b, ok := d.av2.(*dyn2Types.AttributeValueMemberB); ok {
			return b.Value, true
		}
	}
	return nil, false
}

func (d decoder) ListLen() (int, bool) {
	if d.av1 != nil {
		if d.av1.L == nil {
			return 0, false
		}
		return len(d.av1.L), true
	}
	if d.av2 != nil {
		if l, ok := d.av2.(*dyn2Types.AttributeValueMemberL); ok {
			if l.Value == nil {
				return 0, false
			}
			return len(l.Value), true
		}
	}
	return 0, false
}

func (d decoder) DecodeList(f func(i int, vd driver.Decoder) bool) {
	if d.av1 != nil {
		for i, el := range d.av1.L {
			if !f(i, decoder{av1: el}) {
				break
			}
		}
	}
	if d.av2 != nil {
		if l, ok := d.av2.(*dyn2Types.AttributeValueMemberL); ok {
			for i, el := range l.Value {
				if !f(i, decoder{av2: el}) {
					break
				}
			}
		}
	}
}

func (d decoder) MapLen() (int, bool) {
	if d.av1 != nil {
		if d.av1.M == nil {
			return 0, false
		}
		return len(d.av1.M), true
	}
	if d.av2 != nil {
		if m, ok := d.av2.(*dyn2Types.AttributeValueMemberM); ok {
			if m.Value == nil {
				return 0, false
			}
			return len(m.Value), true
		}
	}
	return 0, false
}

func (d decoder) DecodeMap(f func(key string, vd driver.Decoder, exactMatch bool) bool) {
	if d.av1 != nil {
		for k, av := range d.av1.M {
			if !f(k, decoder{av1: av}, true) {
				break
			}
		}
	}
	if d.av2 != nil {
		if m, ok := d.av2.(*dyn2Types.AttributeValueMemberM); ok {
			for k, av := range m.Value {
				if !f(k, decoder{av2: av}, true) {
					break
				}
			}
		}
	}
}

func (d decoder) AsInterface() (interface{}, error) {
	if d.av1 != nil {
		return toGoValue(d.av1)
	}
	if d.av2 != nil {
		return toGoValueV2(d.av2)
	}
	return nil, nil
}

func toGoValue(av *dyn.AttributeValue) (interface{}, error) {
	switch {
	case av.NULL != nil:
		return nil, nil
	case av.BOOL != nil:
		return *av.BOOL, nil
	case av.N != nil:
		f, err := strconv.ParseFloat(*av.N, 64)
		if err != nil {
			return nil, err
		}
		i := int64(f)
		if float64(i) == f {
			return i, nil
		}
		u := uint64(f)
		if float64(u) == f {
			return u, nil
		}
		return f, nil

	case av.B != nil:
		return av.B, nil
	case av.S != nil:
		return *av.S, nil

	case av.L != nil:
		s := make([]interface{}, len(av.L))
		for i, v := range av.L {
			x, err := toGoValue(v)
			if err != nil {
				return nil, err
			}
			s[i] = x
		}
		return s, nil

	case av.M != nil:
		m := make(map[string]interface{}, len(av.M))
		for k, v := range av.M {
			x, err := toGoValue(v)
			if err != nil {
				return nil, err
			}
			m[k] = x
		}
		return m, nil

	default:
		return nil, fmt.Errorf("awsdynamodb: AttributeValue %s not supported", av)
	}
}

func toGoValueV2(av dyn2Types.AttributeValue) (interface{}, error) {
	var err error

	switch t := av.(type) {
	case *dyn2Types.AttributeValueMemberB:
		return t.Value, nil
	case *dyn2Types.AttributeValueMemberBOOL:
		return t.Value, nil
	case *dyn2Types.AttributeValueMemberBS:
		return t.Value, nil
	case *dyn2Types.AttributeValueMemberL:
		l := make([]any, len(t.Value))
		for i, v := range t.Value {
			l[i], err = toGoValueV2(v)
			if err != nil {
				return l, err
			}
		}
		return l, nil
	case *dyn2Types.AttributeValueMemberM:
		m := make(map[string]any, len(t.Value))
		for k, v := range t.Value {
			m[k], err = toGoValueV2(v)
			if err != nil {
				return m, err
			}
		}
		return m, err
	case *dyn2Types.AttributeValueMemberN:
		f, err := strconv.ParseFloat(t.Value, 64)
		if err != nil {
			return nil, err
		}
		i := int64(f)
		if float64(i) == f {
			return i, nil
		}
		u := uint64(f)
		if float64(u) == f {
			return u, nil
		}
		return f, nil
	case *dyn2Types.AttributeValueMemberNS:
		return t.Value, nil
	case *dyn2Types.AttributeValueMemberNULL:
		return nil, nil
	case *dyn2Types.AttributeValueMemberS:
		return t.Value, nil
	case *dyn2Types.AttributeValueMemberSS:
		return t.Value, nil

	default:
		return nil, fmt.Errorf("awsdynamodb: AttributeValue %s not supported", av)
	}
}

func (d decoder) AsSpecial(v reflect.Value) (bool, interface{}, error) {
	if d.av1 != nil {
		return d.asV1Special(v)
	}
	if d.av2 != nil {
		return d.asV2Special(v)
	}
	return false, nil, nil
}

func (d decoder) asV1Special(v reflect.Value) (bool, interface{}, error) {
	unsupportedTypes := `unsupported type, the docstore driver for DynamoDB does
	not decode DynamoDB set types, such as string set, number set and binary set`
	if d.av1.SS != nil || d.av1.NS != nil || d.av1.BS != nil {
		return true, nil, errors.New(unsupportedTypes)
	}
	switch v.Type() {
	case typeOfGoTime:
		if d.av1.S == nil {
			return false, nil, errors.New("expected string field for time.Time")
		}
		t, err := time.Parse(time.RFC3339Nano, *d.av1.S)
		return true, t, err
	}
	return false, nil, nil
}

func (d decoder) asV2Special(v reflect.Value) (bool, interface{}, error) {
	unsupportedTypes := `unsupported type, the docstore driver for DynamoDB does
	not decode DynamoDB set types, such as string set, number set and binary set`
	switch d.av2.(type) {
	case *dyn2Types.AttributeValueMemberSS:
		return true, nil, errors.New(unsupportedTypes)
	case *dyn2Types.AttributeValueMemberNS:
		return true, nil, errors.New(unsupportedTypes)
	case *dyn2Types.AttributeValueMemberBS:
		return true, nil, errors.New(unsupportedTypes)
	}

	switch v.Type() {
	case typeOfGoTime:
		if _, ok := d.av2.(*dyn2Types.AttributeValueMemberNULL); ok {
			return false, nil, errors.New("expected string field for time.Time")
		}
		if s, ok := d.av2.(*dyn2Types.AttributeValueMemberS); ok {
			t, err := time.Parse(time.RFC3339Nano, s.Value)
			return true, t, err
		}
	}

	return false, nil, nil
}
