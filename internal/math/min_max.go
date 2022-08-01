package math

func IntMin(x, y int) int {
	if x > y {
		return y
	}
	return x
}

func IntMax(x, y int) int {
	if x > y {
		return x
	}
	return y
}
