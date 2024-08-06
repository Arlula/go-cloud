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
	"reflect"
	"testing"

	dyn2Types "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	dyn "github.com/aws/aws-sdk-go/service/dynamodb"
	dynattr "github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"gocloud.dev/docstore/driver"
	"gocloud.dev/docstore/drivertest"
)

func TestEncodeValue(t *testing.T) {
	av := func() *dyn.AttributeValue { return &dyn.AttributeValue{} }
	avn := func(s string) *dyn.AttributeValue { return av().SetN(s) }
	avl := func(avs ...*dyn.AttributeValue) *dyn.AttributeValue { return av().SetL(avs) }

	var seven int32 = 7
	var nullptr *int

	for _, test := range []struct {
		in     interface{}
		wantV1 *dyn.AttributeValue
		wantV2 dyn2Types.AttributeValue
	}{
		// null
		{nil, nullValue, &dyn2Types.AttributeValueMemberNULL{}},
		{nullptr, nullValue, &dyn2Types.AttributeValueMemberNULL{}},
		// number
		{0, avn("0"), &dyn2Types.AttributeValueMemberN{Value: "0"}},
		{uint64(999), avn("999"), &dyn2Types.AttributeValueMemberN{Value: "999"}},
		{3.5, avn("3.5"), &dyn2Types.AttributeValueMemberN{Value: "3.5"}},
		{seven, avn("7"), &dyn2Types.AttributeValueMemberN{Value: "7"}},
		{&seven, avn("7"), &dyn2Types.AttributeValueMemberN{Value: "7"}},
		// string
		{"", nullValue, &dyn2Types.AttributeValueMemberNULL{}},
		{"x", av().SetS("x"), &dyn2Types.AttributeValueMemberS{Value: "x"}},
		// bool
		{true, av().SetBOOL(true), &dyn2Types.AttributeValueMemberBOOL{Value: true}},
		// list
		{[]int(nil), nullValue, &dyn2Types.AttributeValueMemberNULL{}},
		{[]int{}, av().SetL([]*dyn.AttributeValue{}), &dyn2Types.AttributeValueMemberL{Value: []dyn2Types.AttributeValue{}}},
		{[]int{1, 2}, avl(avn("1"), avn("2")), &dyn2Types.AttributeValueMemberL{Value: []dyn2Types.AttributeValue{&dyn2Types.AttributeValueMemberN{Value: "1"}, &dyn2Types.AttributeValueMemberN{Value: "2"}}}},
		{[...]int{1, 2}, avl(avn("1"), avn("2")), &dyn2Types.AttributeValueMemberL{Value: []dyn2Types.AttributeValue{&dyn2Types.AttributeValueMemberN{Value: "1"}, &dyn2Types.AttributeValueMemberN{Value: "2"}}}},
		// map
		{[]interface{}{nil, false}, avl(nullValue, av().SetBOOL(false)), &dyn2Types.AttributeValueMemberL{Value: []dyn2Types.AttributeValue{&dyn2Types.AttributeValueMemberNULL{}, &dyn2Types.AttributeValueMemberBOOL{}}}},
		{map[string]int(nil), nullValue, &dyn2Types.AttributeValueMemberNULL{}},
		{map[string]int{}, av().SetM(map[string]*dyn.AttributeValue{}), &dyn2Types.AttributeValueMemberM{Value: map[string]dyn2Types.AttributeValue{}}},
		{
			map[string]int{"a": 1, "b": 2},
			av().SetM(map[string]*dyn.AttributeValue{
				"a": avn("1"),
				"b": avn("2"),
			}),
			&dyn2Types.AttributeValueMemberM{Value: map[string]dyn2Types.AttributeValue{"a": &dyn2Types.AttributeValueMemberN{Value: "1"}, "b": &dyn2Types.AttributeValueMemberN{Value: "2"}}},
		},
	} {
		var e encoder
		if err := driver.Encode(reflect.ValueOf(test.in), &e); err != nil {
			t.Fatal(err)
		}
		gotV1, err := e.asV1AttributeValue()
		if err != nil {
			t.Errorf("%#v: failed encoding as V1 attribute value", test.in)
		}
		if !cmp.Equal(gotV1, test.wantV1, cmpopts.IgnoreUnexported(dyn.AttributeValue{})) {
			t.Errorf("%#v: got %#v, want %#v", test.in, gotV1, test.wantV1)
		}

		gotV2, err := e.asV2AttributeValue()
		if err != nil {
			t.Errorf("%#v: failed encoding as V1 attribute value", test.in)
		}
		if !cmp.Equal(gotV2, test.wantV2, cmpopts.IgnoreUnexported(
			dyn2Types.AttributeValueMemberB{},
			dyn2Types.AttributeValueMemberBOOL{},
			dyn2Types.AttributeValueMemberBS{},
			dyn2Types.AttributeValueMemberL{},
			dyn2Types.AttributeValueMemberM{},
			dyn2Types.AttributeValueMemberN{},
			dyn2Types.AttributeValueMemberNS{},
			dyn2Types.AttributeValueMemberNULL{},
			dyn2Types.AttributeValueMemberS{},
			dyn2Types.AttributeValueMemberSS{},
		)) {
			t.Errorf("%#v: got %#v, want %#v", test.in, gotV2, test.wantV2)
		}
	}
}

func TestDecodeValue(t *testing.T) {
	av := func() *dyn.AttributeValue { return &dyn.AttributeValue{} }
	avn := func(s string) *dyn.AttributeValue { return av().SetN(s) }
	avl := func(vals ...*dyn.AttributeValue) *dyn.AttributeValue { return av().SetL(vals) }
	avn2 := func(s string) dyn2Types.AttributeValue { return &dyn2Types.AttributeValueMemberN{Value: s} }
	avl2 := func(vals ...dyn2Types.AttributeValue) dyn2Types.AttributeValue {
		return &dyn2Types.AttributeValueMemberL{Value: vals}
	}
	for _, tc := range []struct {
		inV1 *dyn.AttributeValue
		inV2 dyn2Types.AttributeValue
		out  any
	}{
		// NULL
		// {nullValue, nil}, // TODO: cant reflect new, how to test?
		// bool
		{av().SetBOOL(false), &dyn2Types.AttributeValueMemberBOOL{Value: false}, false},
		{av().SetBOOL(true), &dyn2Types.AttributeValueMemberBOOL{Value: true}, true},
		// string
		{av().SetS("x"), &dyn2Types.AttributeValueMemberS{Value: "x"}, "x"},
		// int64
		{avn("7"), avn2("7"), int64(7)},
		{avn("-7"), avn2("-7"), int64(-7)},
		{avn("0"), avn2("0"), int64(0)},
		// uint64
		{avn("7"), avn2("7"), uint64(7)},
		{avn("0"), avn2("0"), uint64(0)},
		// float64
		{avn("7"), avn2("7"), float64(7)},
		{avn("0"), avn2("0"), float64(0)},
		{avn("3.1415"), avn2("3.1415"), float64(3.1415)},
		// complex128
		// TODO: fails at driver
		// {av().SetL([]*dyn.AttributeValue{avn("12"), avn("37")}), complex(12, 37)},
		// []byte
		{av().SetB([]byte(`123`)), &dyn2Types.AttributeValueMemberB{Value: []byte(`123`)}, []byte(`123`)},
		// List
		{avl(avn("12"), avn("37")), avl2(avn2("12"), avn2("37")), []int64{12, 37}},
		{avl(avn("12"), avn("37")), avl2(avn2("12"), avn2("37")), []uint64{12, 37}},
		{avl(avn("12.8"), avn("37.1")), avl2(avn2("12.8"), avn2("37.1")), []float64{12.8, 37.1}},
		// Map
		{
			av().SetM(map[string]*dyn.AttributeValue{}),
			&dyn2Types.AttributeValueMemberM{Value: map[string]dyn2Types.AttributeValue{}},
			map[string]int{},
		},
		{
			av().SetM(map[string]*dyn.AttributeValue{"a": avn("1"), "b": avn("2")}),
			&dyn2Types.AttributeValueMemberM{Value: map[string]dyn2Types.AttributeValue{"a": avn2("1"), "b": avn2("2")}},
			map[string]int{"a": 1, "b": 2},
		},
	} {
		var (
			d      = decoder{av1: tc.inV1}
			target = reflect.New(reflect.TypeOf(tc.out))
		)

		if err := driver.Decode(target.Elem(), &d); err != nil {
			t.Errorf("[V1] error decoding value %#v, got error: %#v", tc.inV1, err)
			continue
		}

		if !cmp.Equal(target.Elem().Interface(), tc.out) {
			t.Errorf("[V1] %#v: got %#v, want %#v", tc.inV1, target.Elem().Interface(), tc.out)
		}

		d = decoder{av2: tc.inV2}
		if err := driver.Decode(target.Elem(), &d); err != nil {
			t.Errorf("[V2] error decoding value %#v, got error: %#v", tc.inV2, err)
			continue
		}

		if !cmp.Equal(target.Elem().Interface(), tc.out) {
			t.Errorf("[V2] %#v: got %#v, want %#v", tc.inV2, target.Elem().Interface(), tc.out)
		}
	}
}

func TestDecodeErrorOnUnsupported(t *testing.T) {
	av := func() *dyn.AttributeValue { return &dyn.AttributeValue{} }
	sptr := func(s string) *string { return &s }
	for _, tc := range []struct {
		in  *dyn.AttributeValue
		out interface{}
	}{
		{av().SetSS([]*string{sptr("foo"), sptr("bar")}), []string{}},
		{av().SetNS([]*string{sptr("1.1"), sptr("-2.2"), sptr("3.3")}), []float64{}},
		{av().SetBS([][]byte{{4}, {5}, {6}}), [][]byte{}},
	} {
		d := decoder{av1: tc.in}
		if err := driver.Decode(reflect.ValueOf(tc.out), &d); err == nil {
			t.Error("got nil error, want unsupported error")
		}
	}
}

type codecTester struct{}

func (ct *codecTester) UnsupportedTypes() []drivertest.UnsupportedType {
	return []drivertest.UnsupportedType{drivertest.BinarySet}
}

func (ct *codecTester) NativeEncode(obj interface{}) (interface{}, error) {
	return dynattr.Marshal(obj)
}

func (ct *codecTester) NativeDecode(value, dest interface{}) error {
	return dynattr.Unmarshal(value.(*dyn.AttributeValue), dest)
}

func (ct *codecTester) DocstoreEncode(obj interface{}) (interface{}, error) {
	return encodeDoc(drivertest.MustDocument(obj))
}

func (ct *codecTester) DocstoreDecode(value, dest interface{}) error {
	return decodeDoc(value.(*dyn.AttributeValue), drivertest.MustDocument(dest))
}
