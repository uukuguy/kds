package store

import "github.com/uukuguy/kds/haystack"

// **************** Cluster ****************
type Cluster struct {
	DataCenters []*DataCenter
}

// **************** DataCenter ****************
type DataCenter struct {
	Racks []*Rack
}

// **************** Rack ****************
type Rack struct {
	Hosts []*Host
}

// **************** Rack ****************
type Host struct {
	Name string
	IP   string
}

func (this *Cluster) GetWritableVolumes() (volume *haystack.Volume, err error) {
	return
}
