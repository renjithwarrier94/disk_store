package metadataHandler

//All methods related to metadataLookup file

import (
    "os"
    "syscall"
    "github.com/pkg/errors"
    "math"
    "fmt"
    "unsafe"
    "sync/atomic"
)

type metadataOutOfSlots struct {
    current_num_slots uint32
}

func (o metadataOutOfSlots) Error() string {
    return fmt.Sprintf("Could not find a free slot among %v slots", o.current_num_slots)
}

type metadataLookupNumRetriesExceeded struct {
    slot_num    uint32
    num_retries int
}

func (m metadataLookupNumRetriesExceeded) Error() string {
    return fmt.Sprintf("Could not reserve slot %v even after trying for %v times", m.slot_num,
m.num_retries)
}

//The values for each filled slot in a byte
var SLOT_OCCUPIED_VALUES  = [8]byte{0x80, 0x40, 0x20, 0x10, 0x08, 0x04, 0x02, 0x01}

//Create/open a metadata lookup file
func (metadata *Metadata) createMetadataLookupFile(path string) error {
    var f *os.File
    //To fill up the lookup file with empty bytes, we need the old and new size. For new file, old_size is 0
    var old_size, min_req_size int64
    //Set the size to the min required size - each byte in lookup can represent 8 slots
    //Also since atomic operations require atleast 4 bytes, the size should be a multiple of 4 bytes
    min_req_size = int64(math.Ceil( math.Ceil(float64(metadata.num_slots)/8.0) / 4.0 ) * 4)
    //Open/create the file
    f, err := os.OpenFile(path + metadataLookupFileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0777)
    if err != nil {
        return errors.Wrap(err, "Could not create metadata lookup file")
    }
    //Get the size of the file
    info, err := f.Stat()
    old_size = info.Size()
    if err != nil {
        return errors.Wrap(err, "Could not obtain the stats for metadata lookup file")
    }
    //If the file needs to be truncated, do that
    if  old_size < min_req_size {
        err = f.Truncate(min_req_size)
        if err != nil {
            return errors.Wrap(err, "Could not truncate metadata lookup file")
        }
    }
    //Add it to the metadata struct
    metadata.lookup_file = f
    //Create mmap
    var d []byte
    if old_size < min_req_size {
        d, err = syscall.Mmap(int(f.Fd()), 0, int(min_req_size), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
    } else {
        d, err = syscall.Mmap(int(f.Fd()), 0, int(old_size), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
    }
    if err != nil {
        return errors.Wrap(err, "Could not successfully create memory mapping over metadata lookup file")
    }
    metadata.lookup = d
    //Initialize the file
    metadata.initializeLookupFile(old_size, min_req_size)
    return nil
}

func (metadata *Metadata) initializeLookupFile (old_size, new_size int64) {
    for i:=old_size; i<new_size; i++ {
        metadata.lookup[i] = byte(0x00)
    }
}

func (m *Metadata) reserveSlot(byteNo uint32, offset int) bool {
    //Get the greatest lowest multiple of 4
    start_byte := uint32(math.Floor(float64(byteNo) / 4.0)) * 4
    current_byte_position := byteNo % 4
    //Get these 4 bytes from file
    first_4_bytes := make([]byte, 4)
    //m.log.Infof("ByteNo: %v, start_byte: %v, current_byte_position: %v", byteNo, start_byte, current_byte_position)
    copy(first_4_bytes, m.lookup[start_byte: start_byte + 4])
    //Convert it ti uint32
    old_data := m.byteOrder.Uint32(first_4_bytes)
    //Set data to the new value
    first_4_bytes[current_byte_position] = first_4_bytes[current_byte_position] | SLOT_OCCUPIED_VALUES[offset]
    //Convert it to uint32
    new_data := m.byteOrder.Uint32(first_4_bytes)
    //Convert *byte to *uint32
    var ptr *uint32 = (*uint32)(unsafe.Pointer(&m.lookup[start_byte]))
    res := atomic.CompareAndSwapUint32(ptr, old_data, new_data)
    return res
}

func (m *Metadata) getOpenSlot() (uint32, error) {
    //Set the number of times to retry if compare and swap fails
    num_retries := 5
    //Iterate thorugh all the bytes in the lookup file to find open slots
    //TODO: parallelize this
    largest_byte := uint32(math.Floor(float64(m.num_slots) / 8.0))
    //Iterate through the bytes to find an open slot
    for i:=uint32(0); i<largest_byte; i++ {
        //If the byte is 0xff, no point in continuing as its all occupied
        //If not continue
        if m.lookup[i] != 0xff {
            //Find which bit is free
            //Iterate through SLOT_OCCUPIED_VALUES and find which slot is empty
            for j:=0; j<8; j++ {
                //Check if the current slot is within limits
                if i*8 + uint32(j) > m.num_slots {
                    //Return out of slots
                    return 0, metadataOutOfSlots {current_num_slots: m.num_slots}
                }
                //Else check if it is free
                if m.lookup[i] & SLOT_OCCUPIED_VALUES[j] == 0x00 {
                    //Reserve the slot
                    res := m.reserveSlot(i, j)
                    if res {
                        //If it was reserved
                        return i*8 + uint32(j) , nil
                    }
                    k := 0
                    //If not, return an error
                    //return 0, errors.Errorf("Could not set a slot")
                    //If not, keep retrying for num_retries
                    for k=0; k<num_retries; k++ {
                        //Check if the slot is free again
                        //If not, continue on to the next slot
                        if m.lookup[i] & SLOT_OCCUPIED_VALUES[j] != 0x00 {
                            break
                        }
                        res = m.reserveSlot(i, j)
                        if res {
                            //We were able to block. so return
                            return i*8 + uint32(j), nil
                        }
                    }
                    if k >= num_retries {
                        //Num retries exceeded, so return that error
                        return 0, metadataLookupNumRetriesExceeded {
                            slot_num     : i*8 + uint32(j),
                            num_retries  : num_retries,
                        }
                    }
                }
            }
        }
    }
    //Say that we are out of slots
    return 0, metadataOutOfSlots {current_num_slots: m.num_slots}
}
