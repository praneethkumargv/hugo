package main

import "github.com/draffensperger/golp"

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
	for i := 0; i < noofpm; i++ {
		for j := 0; j < noofvm; j++ {
			objfn = append(objfn, a)
		}
	}
	for i := 0; i < noofpm; i++ {
		objfn = append(objfn, b)
	}
	lp.SetObjFn(objfn)
	lp.Solve()

	vars := lp.Variables()
	m := make(map[int]int)

	for i := 0; i < noofvm; i++ {
		for j := 0; j < noofpm; j++ {
			if vars[i*noofpm+j] == 1 {
				m[i] = j
			}
		}
	}
	return m
}

// func main() {
// 	lp := golp.NewLP(0, 2)
// 	lp.AddConstraint([]float64{1, 1}, golp.LE, 1000)
// 	lp.AddConstraint([]float64{3, 2}, golp.LE, 2300)
// 	lp.SetObjFn([]float64{6, 5})
// 	lp.SetMaximize()

// 	lp.Solve()
// 	vars := lp.Variables()
// 	fmt.Printf("Plant %.3f acres of barley\n", vars[0])
// 	fmt.Printf("And  %.3f acres of wheat\n", vars[1])
// 	fmt.Printf("For optimal profit of $%.2f\n", lp.Objective())

// 	// No need to explicitly free underlying C structure as golp.LP finalizer will
// }
