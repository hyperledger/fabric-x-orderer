/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package protolator

import (
	"reflect"

	"google.golang.org/protobuf/proto"
)

type variablyOpaqueFieldFactory struct{}

func (soff variablyOpaqueFieldFactory) Handles(msg proto.Message, fieldName string, fieldType reflect.Type, fieldValue reflect.Value) bool {
	opaqueProto, ok := msg.(VariablyOpaqueFieldProto)
	if !ok {
		return false
	}

	return stringInSlice(fieldName, opaqueProto.VariablyOpaqueFields())
}

func (soff variablyOpaqueFieldFactory) NewProtoField(msg proto.Message, fieldName string, fieldType reflect.Type, fieldValue reflect.Value) (protoField, error) {
	opaqueProto := msg.(VariablyOpaqueFieldProto) // Type checked in Handles

	return &plainField{
		baseField: baseField{
			msg:   msg,
			name:  fieldName,
			fType: mapStringInterfaceType,
			vType: bytesType,
			value: fieldValue,
		},
		populateFrom: func(v interface{}, dT reflect.Type) (reflect.Value, error) {
			return opaqueFrom(func() (proto.Message, error) { return opaqueProto.VariablyOpaqueFieldProto(fieldName) }, v, dT)
		},
		populateTo: func(v reflect.Value) (interface{}, error) {
			return opaqueTo(func() (proto.Message, error) { return opaqueProto.VariablyOpaqueFieldProto(fieldName) }, v)
		},
	}, nil
}

type variablyOpaqueMapFieldFactory struct{}

func (soff variablyOpaqueMapFieldFactory) Handles(msg proto.Message, fieldName string, fieldType reflect.Type, fieldValue reflect.Value) bool {
	opaqueProto, ok := msg.(VariablyOpaqueMapFieldProto)
	if !ok {
		return false
	}

	return stringInSlice(fieldName, opaqueProto.VariablyOpaqueMapFields())
}

func (soff variablyOpaqueMapFieldFactory) NewProtoField(msg proto.Message, fieldName string, fieldType reflect.Type, fieldValue reflect.Value) (protoField, error) {
	opaqueProto := msg.(VariablyOpaqueMapFieldProto) // Type checked in Handles

	return &mapField{
		baseField: baseField{
			msg:   msg,
			name:  fieldName,
			fType: mapStringInterfaceType,
			vType: fieldType,
			value: fieldValue,
		},
		populateFrom: func(key string, v interface{}, dT reflect.Type) (reflect.Value, error) {
			return opaqueFrom(func() (proto.Message, error) {
				return opaqueProto.VariablyOpaqueMapFieldProto(fieldName, key)
			}, v, dT)
		},
		populateTo: func(key string, v reflect.Value) (interface{}, error) {
			return opaqueTo(func() (proto.Message, error) {
				return opaqueProto.VariablyOpaqueMapFieldProto(fieldName, key)
			}, v)
		},
	}, nil
}

type variablyOpaqueSliceFieldFactory struct{}

func (soff variablyOpaqueSliceFieldFactory) Handles(msg proto.Message, fieldName string, fieldType reflect.Type, fieldValue reflect.Value) bool {
	opaqueProto, ok := msg.(VariablyOpaqueSliceFieldProto)
	if !ok {
		return false
	}

	return stringInSlice(fieldName, opaqueProto.VariablyOpaqueSliceFields())
}

func (soff variablyOpaqueSliceFieldFactory) NewProtoField(msg proto.Message, fieldName string, fieldType reflect.Type, fieldValue reflect.Value) (protoField, error) {
	opaqueProto := msg.(VariablyOpaqueSliceFieldProto) // Type checked in Handles

	return &sliceField{
		baseField: baseField{
			msg:   msg,
			name:  fieldName,
			fType: mapStringInterfaceType,
			vType: fieldType,
			value: fieldValue,
		},
		populateFrom: func(index int, v interface{}, dT reflect.Type) (reflect.Value, error) {
			return opaqueFrom(func() (proto.Message, error) {
				return opaqueProto.VariablyOpaqueSliceFieldProto(fieldName, index)
			}, v, dT)
		},
		populateTo: func(index int, v reflect.Value) (interface{}, error) {
			return opaqueTo(func() (proto.Message, error) {
				return opaqueProto.VariablyOpaqueSliceFieldProto(fieldName, index)
			}, v)
		},
	}, nil
}
