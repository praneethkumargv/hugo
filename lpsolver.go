package main

import (
	"github.com/draffensperger/golp"
)

const (
	a = 550
	b = 4260
)

func Solve(pcpu []uint32, pmemory []uint32, ccpu uint32, cmem uint32, scpu []uint32, smem []uint32) map[int]int {

	// first overloaded pm
	// next 5 on pm
	// next 5 sleep pm

	noofvm := len(pcpu)
	noofpm := len(scpu) + 1
	noofcols := noofvm*noofpm + noofpm
	lp := golp.NewLP(0, noofcols)
	for i := 0; i < noofcols; i++ {
		lp.SetBinary(i, true)
	}
	for i := 0; i < noofvm; i++ {
		var row []golp.Entry
		for j := 0; j < noofpm; j++ {
			entry := golp.Entry{Col: i*noofpm + j, Val: 1.0}
			row = append(row, entry)
		}
		lp.AddConstraintSparse(row, golp.EQ, 1.0)
	}
	row := make([]golp.Entry, 0)
	for i := 0; i < noofvm; i++ {
		entry := golp.Entry{Col: i * noofpm, Val: float64(pcpu[i])}
		row = append(row, entry)
	}
	lp.AddConstraintSparse(row, golp.LE, float64(ccpu))

	row = make([]golp.Entry, 0)
	for i := 0; i < noofvm; i++ {
		entry := golp.Entry{Col: i * noofpm, Val: float64(pmemory[i])}
		row = append(row, entry)
	}
	lp.AddConstraintSparse(row, golp.LE, float64(cmem))

	for j := 1; j < noofpm; j++ {
		var row []golp.Entry
		for i := 0; i < noofvm; i++ {
			entry := golp.Entry{Col: i*noofpm + j, Val: float64(pcpu[i])}
			row = append(row, entry)
		}
		lp.AddConstraintSparse(row, golp.LE, float64(scpu[j-1]))
	}
	for j := 1; j < noofpm; j++ {
		var row []golp.Entry
		for i := 0; i < noofvm; i++ {
			entry := golp.Entry{Col: i*noofpm + j, Val: float64(pmemory[i])}
			row = append(row, entry)
		}
		lp.AddConstraintSparse(row, golp.LE, float64(smem[j-1]))
	}
	for j := 0; j < noofpm; j++ {
		var row []golp.Entry
		for i := 0; i < noofvm; i++ {
			entry := golp.Entry{Col: i*noofpm + j, Val: 1.0}
			row = append(row, entry)
		}
		entry := golp.Entry{Col: noofpm*noofvm + j, Val: -1.0 * float64(noofvm)}
		row = append(row, entry)
		lp.AddConstraintSparse(row, golp.LE, 0)
	}

	for j := 0; j < noofpm; j++ {
		var row []golp.Entry
		for i := 0; i < noofvm; i++ {
			entry := golp.Entry{Col: i*noofpm + j, Val: 1.0}
			row = append(row, entry)
		}
		entry := golp.Entry{Col: noofpm*noofvm + j, Val: -1}
		row = append(row, entry)
		lp.AddConstraintSparse(row, golp.GE, -1)
	}

	var objfn []float64
	for i := 0; i < noofvm; i++ {
		for j := 0; j < noofpm; j++ {
			if j == 0 {
				objfn = append(objfn, 0)
				continue
			}
			objfn = append(objfn, a)
		}
	}
	for i := 0; i < noofpm; i++ {
		objfn = append(objfn, 0)
	}
	lp.SetObjFn(objfn)
	lp.Solve()

	vars := lp.Variables()
	m := make(map[int]int)

	for i := 0; i < noofvm; i++ {
		for j := 0; j < noofpm; j++ {
			print(vars[i*noofpm+j])
			if vars[i*noofpm+j] == 1 {
				m[i] = j
			}
		}
	}
	return m
}

// func main() {

// 	var fpcpu = []float64{0.0007257, 0.0001993, 0.05243, 0.01752, 0.03925, 0.05762, 0.03497, 0.03961, 0.1078, 0.01859, 0.009425, 0.005539, 0.006706}
// 	var fpmem = []float64{0.0067905, 0.0024359, 0.02905, 0.016785, 0.02928, 0.02655, 0.014222, 0.02902, 0.02826, 0.009425, 0.004741, 0.13245, 0.12488}
// 	var ccpu, cmem uint32
// 	var scpu = []uint32{326262, 442258, 254265}
// 	var smem = []uint32{808411, 501363, 251155}
// 	// for i := 0; i < 17; i++ {
// 	// 	pcpu = append(pcpu, uint32(0.775*float64(1000)))
// 	// 	pmem = append(pmem, 256)
// 	// }
// 	ccpu = uint32(0.5 * float64(1000000))
// 	cmem = uint32(0.19999 * float64(1000000))

// 	pcpu := make([]uint32, 0)
// 	pmem := make([]uint32, 0)
// 	for i, ele := range fpcpu {
// 		pcpu = append(pcpu, uint32(ele*float64(1000000)))
// 		pmem = append(pmem, uint32(fpmem[i]*float64(1000000)))
// 	}

// 	for _, ele := range pcpu {
// 		fmt.Println(ele)
// 	}

// 	ans := Solve(pcpu, pmem, ccpu, cmem, scpu, smem)
// 	for vm, pm := range ans {
// 		fmt.Println(vm, pm)
// 	}
// }
