package server

import (
	"reflect"
	"testing"
)

func TestLog_Append(t *testing.T) {
	type fields struct {
		records []Record
	}
	type args struct {
		record Record
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    uint64
		wantErr bool
	}{
		{
			"Append",
			fields{
				records: NewLog().records,
			},
			args{
				Record{
					[]byte("test log"),
					0,
				},
			},
			0,
			false,
		},
		{
			"Append",
			fields{
				records: []Record{
					{[]byte("test log"), 0},
				},
			},
			args{
				Record{
					[]byte("test log2"), 0,
				},
			},
			1,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Log{
				records: tt.fields.records,
			}
			got, err := c.Append(tt.args.record)
			if (err != nil) != tt.wantErr {
				t.Errorf("Log.Append() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Log.Append() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLog_Read(t *testing.T) {
	type fields struct {
		records []Record
	}
	type args struct {
		offset uint64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    Record
		wantErr bool
	}{
		{
			"Success to read",
			fields{
				records: []Record{
					{[]byte("test log"), 0},
				},
			},
			args{0},
			Record{[]byte("test log"), 0},
			false,
		},
		{
			"Fail to read",
			fields{
				records: []Record{
					{[]byte("test log"), 0},
				},
			},
			args{1},
			Record{},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Log{
				records: tt.fields.records,
			}
			got, err := c.Read(tt.args.offset)
			if (err != nil) != tt.wantErr {
				t.Errorf("Log.Read() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Log.Read() = %v, want %v", got, tt.want)
			}
		})
	}
}
