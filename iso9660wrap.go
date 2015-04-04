package main


import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strings"
	"time"
)

func Panicf(format string, v ...interface{}) {
	panic(fmt.Errorf(format, v...))
}

const volumeDescriptorSetMagic = "\x43\x44\x30\x30\x31\x01"

const primaryVolumeSectorNum uint32 = 16
const numVolumeSectors uint32 = 2 // primary + terminator
const littleEndianPathTableSectorNum uint32 = primaryVolumeSectorNum + numVolumeSectors
const bigEndianPathTableSectorNum uint32 = littleEndianPathTableSectorNum + 1
const numPathTableSectors = 2 // no secondaries
const rootDirectorySectorNum uint32 = primaryVolumeSectorNum + numVolumeSectors + numPathTableSectors

func printUsage() {
	fmt.Fprintf(os.Stderr, "usage: %s INFILE OUTFILE\n", os.Args[0])
}

func main() {
	if len(os.Args) == 2 && os.Args[1] == "--help" {
		printUsage()
		os.Exit(0)
	} else if len(os.Args) != 3 {
		printUsage()
		os.Exit(1)
	}

	log.SetFlags(0)

	infile := os.Args[1]
	outfile := os.Args[2]

	outfh, err := os.OpenFile(outfile, os.O_CREATE | os.O_EXCL | os.O_WRONLY, 0666)
	if err != nil {
		log.Fatalf("could not open output file %s for writing: %s", outfile, err)
	}
	infh, err := os.Open(infile)
	if err != nil {
		log.Fatalf("could not open input file %s for reading: %s", infile, err)
	}
	inputFileSize, inputFilename, err := getInputFileSizeAndName(infh)
	if err != nil {
		log.Fatalf("could not read from input file %s: %s", infile, err)
	}
	inputFilename = strings.ToUpper(inputFilename)
	if !filenameSatisfiesISOConstraints(inputFilename) {
		log.Fatalf("input file name %s does not satisfy the ISO9660 character set constraints", infile)
	}

	// reserved sectors
	_, err = outfh.Seek(int64(16 * SectorSize), os.SEEK_SET)
	if err != nil {
		log.Fatalf("could not seek output file: %s", err)
	}

	err = nil
	func() {
		defer func() {
			var ok bool
			e := recover()
			if e != nil {
				err, ok = e.(error)
				if !ok {
					panic(e)
				}
			}
		}()

		bufw := bufio.NewWriter(outfh)

		w := NewISO9660Writer(bufw)

		writePrimaryVolumeDescriptor(w, inputFileSize, inputFilename)
		writeVolumeDescriptorSetTerminator(w)
		writePathTable(w, binary.LittleEndian)
		writePathTable(w, binary.BigEndian)
		writeData(w, infh, inputFileSize, inputFilename)

		w.Finish()

		err := bufw.Flush()
		if err != nil {
			panic(err)
		}
	}()
	if err != nil {
		log.Fatalf("could not write to output file: %s", err)
	}
}

func writePrimaryVolumeDescriptor(w *ISO9660Writer, inputFileSize uint32, inputFilename string) {
	if len(inputFilename) > 32 {
		inputFilename = inputFilename[:32]
	}
	now := time.Now()

	sw := w.NextSector()
	if w.CurrentSector() != primaryVolumeSectorNum {
		Panicf("internal error: unexpected primary volume sector %d", w.CurrentSector())
	}

	sw.WriteByte('\x01')
	sw.WriteString(volumeDescriptorSetMagic)
	sw.WriteByte('\x00')

	sw.WritePaddedString("", 32)
	sw.WritePaddedString(inputFilename, 32)

	sw.WriteZeros(8)
	sw.WriteBothEndianDWord(numTotalSectors(inputFileSize))
	sw.WriteZeros(32)

	sw.WriteBothEndianWord(1) // volume set size
	sw.WriteBothEndianWord(1) // volume sequence number
	sw.WriteBothEndianWord(uint16(SectorSize))
	sw.WriteBothEndianDWord(SectorSize) // path table length

	sw.WriteLittleEndianDWord(littleEndianPathTableSectorNum)
	sw.WriteLittleEndianDWord(0) // no secondary path tables
	sw.WriteBigEndianDWord(bigEndianPathTableSectorNum)
	sw.WriteBigEndianDWord(0) // no secondary path tables

	WriteDirectoryRecord(sw, "\x00", rootDirectorySectorNum) // root directory

	sw.WritePaddedString("", 128) // volume set identifier
	sw.WritePaddedString("", 128) // publisher identifier
	sw.WritePaddedString("", 128) // data preparer identifier
	sw.WritePaddedString("", 128) // application identifier

	sw.WritePaddedString("", 37) // copyright file identifier
	sw.WritePaddedString("", 37) // abstract file identifier
	sw.WritePaddedString("", 37) // bibliographical file identifier

	sw.WriteDateTime(now) // volume creation
	sw.WriteDateTime(now) // most recent modification
	sw.WriteUnspecifiedDateTime() // expires
	sw.WriteUnspecifiedDateTime() // is effective (?)

	sw.WriteByte('\x01')
	sw.WriteByte('\x00')

	sw.PadWithZeros() // 512 (reserved for app) + 653 (zeros)
}

func writeVolumeDescriptorSetTerminator(w *ISO9660Writer) {
	sw := w.NextSector()
	if w.CurrentSector() != primaryVolumeSectorNum+1 {
		Panicf("internal error: unexpected volume descriptor set terminator sector %d", w.CurrentSector())
	}

	sw.WriteByte('\xFF')
	sw.WriteString(volumeDescriptorSetMagic)

	sw.PadWithZeros()
}

func writePathTable(w *ISO9660Writer, bo binary.ByteOrder) {
	sw := w.NextSector()
	sw.WriteByte(1) // name length
	sw.WriteByte(0) // number of sectors in extended attribute record
	sw.WriteDWord(bo, rootDirectorySectorNum)
	sw.WriteWord(bo, 1) // parent directory recno (root directory)
	sw.WriteByte(0) // identifier (root directory)
	sw.WriteByte(1) // padding
	sw.PadWithZeros()
}

func writeData(w *ISO9660Writer, infh io.Reader, inputFileSize uint32, inputFilename string) {
	sw := w.NextSector()
	if w.CurrentSector() != rootDirectorySectorNum {
		Panicf("internal error: unexpected root directory sector %d", w.CurrentSector())
	}

	WriteDirectoryRecord(sw, "\x00", w.CurrentSector())
	WriteDirectoryRecord(sw, "\x01", w.CurrentSector())
	WriteFileRecordHeader(sw, inputFilename, w.CurrentSector() + 1, inputFileSize)

	// Now stream the data.  Note that the first buffer is never of SectorSize,
	// since we've already filled a part of the sector.
	b := make([]byte, SectorSize)
	total := uint32(0)
	for {
		l, err := infh.Read(b)
		if err != nil && err != io.EOF {
			Panicf("could not read from input file: %s", err)
		}
		if l > 0 {
			sw = w.NextSector()
			sw.Write(b[:l])
			total += uint32(l)
		}
		if err == io.EOF {
			break
		}
	}
	if total != inputFileSize {
		Panicf("input file size changed while the ISO file was being created (expected to read %d, read %d)", inputFileSize, total)
	} else if w.CurrentSector() != numTotalSectors(inputFileSize) - 1 {
		Panicf("internal error: unexpected last sector number (expected %d, actual %d)",
				numTotalSectors(inputFileSize) - 1, w.CurrentSector())
	}
}

func numTotalSectors(inputFileSize uint32) uint32 {
	numDataSectors := inputFileSize / SectorSize
	if numDataSectors == 0 {
		numDataSectors = 1
	}
	return 1 + bigEndianPathTableSectorNum + 1 + 1 + numDataSectors
}

func getInputFileSizeAndName(fh *os.File) (uint32, string, error) {
	fi, err := fh.Stat()
	if err != nil {
		return 0, "", err
	}
	if fi.Size() >= math.MaxUint32 {
		return 0, "", fmt.Errorf("file size %d is too large", fi.Size())
	}
	return uint32(fi.Size()), fi.Name(), nil
}

func filenameSatisfiesISOConstraints(filename string) bool {
	invalidCharacter := func (r rune) bool {
		// According to ISO9660, only capital letters, digits, and underscores
		// are permitted.
		if r >= 'A' && r <= 'Z' {
			return false
		} else if r >= '0' && r <= '9' {
			return false
		} else if r == '_' {
			return false
		}
		return true
	}
	return strings.IndexFunc(filename, invalidCharacter) == -1
}
