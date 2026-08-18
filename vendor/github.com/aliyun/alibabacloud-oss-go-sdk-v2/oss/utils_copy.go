package oss

import (
	"io"
	"reflect"
	"time"
)

func copyRequest(dst, src interface{}) {
	dstval := reflect.ValueOf(dst)
	if !dstval.IsValid() {
		panic("Copy dst cannot be nil")
	}

	rcopy(dstval, reflect.ValueOf(src), true)
}

func copyOfRequest(src interface{}) (dst interface{}) {
	dsti := reflect.New(reflect.TypeOf(src).Elem())
	dst = dsti.Interface()
	rcopy(dsti, reflect.ValueOf(src), true)
	return
}

func rcopy(dst, src reflect.Value, root bool) {
	if !src.IsValid() {
		return
	}

	switch src.Kind() {
	case reflect.Ptr:
		if _, ok := src.Interface().(io.Reader); ok {
			if dst.Kind() == reflect.Ptr && dst.Elem().CanSet() {
				dst.Elem().Set(src)
			} else if dst.CanSet() {
				dst.Set(src)
			}
		} else {
			e := src.Type().Elem()
			if dst.CanSet() && !src.IsNil() {
				if _, ok := src.Interface().(*time.Time); !ok {
					if dst.Kind() == reflect.String {
						dst.SetString(e.String())
					} else {
						dst.Set(reflect.New(e))
					}
				} else {
					tempValue := reflect.New(e)
					tempValue.Elem().Set(src.Elem())
					dst.Set(tempValue)
				}
			}
			if dst.Kind() != reflect.String && src.Elem().IsValid() {
				rcopy(dst.Elem(), src.Elem(), root)
			}
		}
	case reflect.Struct:
		t := dst.Type()
		for i := 0; i < t.NumField(); i++ {
			name := t.Field(i).Name
			srcVal := src.FieldByName(name)
			dstVal := dst.FieldByName(name)
			if srcVal.IsValid() && dstVal.CanSet() {
				rcopy(dstVal, srcVal, false)
			}
		}
	case reflect.Slice:
		if src.IsNil() {
			break
		}

		s := reflect.MakeSlice(src.Type(), src.Len(), src.Cap())
		dst.Set(s)
		for i := 0; i < src.Len(); i++ {
			rcopy(dst.Index(i), src.Index(i), false)
		}
	case reflect.Map:
		if src.IsNil() {
			break
		}

		s := reflect.MakeMap(src.Type())
		dst.Set(s)
		for _, k := range src.MapKeys() {
			v := src.MapIndex(k)
			v2 := reflect.New(v.Type()).Elem()
			rcopy(v2, v, false)
			dst.SetMapIndex(k, v2)
		}
	default:
		if src.Type().AssignableTo(dst.Type()) {
			dst.Set(src)
		}
	}
}
