package api

import (
	"errors"
	"sync"
	"time"
)

// State — состояние circuit breaker.
type State int

const (
	StateClosed State = iota
	StateHalfOpen
	StateOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateHalfOpen:
		return "half-open"
	case StateOpen:
		return "open"
	default:
		return "unknown"
	}
}

var (
	// ErrOpenState возвращается, когда breaker открыт.
	ErrOpenState = errors.New("circuit breaker is open")
	// ErrTooManyRequests возвращается в half-open, если превышен MaxRequests.
	ErrTooManyRequests = errors.New("too many requests")
)

// Counts — счётчики вызовов в текущем окне.
type Counts struct {
	Requests             uint32
	TotalSuccesses       uint32
	TotalFailures        uint32
	ConsecutiveSuccesses uint32
	ConsecutiveFailures  uint32
}

func (c *Counts) onRequest() {
	c.Requests++
}

func (c *Counts) onSuccess() {
	c.TotalSuccesses++
	c.ConsecutiveSuccesses++
	c.ConsecutiveFailures = 0
}

func (c *Counts) onFailure() {
	c.TotalFailures++
	c.ConsecutiveFailures++
	c.ConsecutiveSuccesses = 0
}

// bucket — один временной срез кольцевого буфера.
type bucket struct {
	slot   int64 // номер временного слота, к которому относится бакет
	valid  bool
	counts Counts
}

// rollingWindow — time-based очередь событий на кольцевом массиве.
// Каждый бакет покрывает bucketWidth времени; окно = len(buckets) * bucketWidth.
type rollingWindow struct {
	buckets     []bucket
	bucketWidth time.Duration
}

func newRollingWindow(numBuckets int, bucketWidth time.Duration) *rollingWindow {
	if numBuckets <= 0 {
		numBuckets = 10
	}
	if bucketWidth <= 0 {
		bucketWidth = time.Nanosecond
	}
	return &rollingWindow{
		buckets:     make([]bucket, numBuckets),
		bucketWidth: bucketWidth,
	}
}

func (w *rollingWindow) slotOf(t time.Time) int64 {
	return t.UnixNano() / int64(w.bucketWidth)
}

// bucketAt возвращает бакет для текущего времени, очищая его при устаревании слота.
func (w *rollingWindow) bucketAt(t time.Time) *bucket {
	slot := w.slotOf(t)
	n := int64(len(w.buckets))
	idx := slot % n
	if idx < 0 {
		idx += n
	}
	b := &w.buckets[idx]
	if !b.valid || b.slot != slot {
		b.valid = true
		b.slot = slot
		b.counts = Counts{}
	}
	return b
}

func (w *rollingWindow) onRequest(t time.Time) {
	w.bucketAt(t).counts.onRequest()
}

func (w *rollingWindow) onSuccess(t time.Time) {
	w.bucketAt(t).counts.onSuccess()
}

func (w *rollingWindow) onFailure(t time.Time) {
	w.bucketAt(t).counts.onFailure()
}

// aggregate суммирует счётчики по всем валидным бакетам окна
// в хронологическом порядке, корректно восстанавливая consecutive-серии.
func (w *rollingWindow) aggregate(t time.Time) Counts {
	curSlot := w.slotOf(t)
	n := int64(len(w.buckets))
	oldest := curSlot - n + 1

	var total Counts
	var cs, cf uint32

	for slot := oldest; slot <= curSlot; slot++ {
		idx := slot % n
		if idx < 0 {
			idx += n
		}
		b := &w.buckets[idx]
		if !b.valid || b.slot != slot {
			continue
		}

		total.Requests += b.counts.Requests
		total.TotalSuccesses += b.counts.TotalSuccesses
		total.TotalFailures += b.counts.TotalFailures

		// Бакет без разрешённых исходов (только in-flight) не рвёт серию.
		if b.counts.TotalSuccesses == 0 && b.counts.TotalFailures == 0 {
			continue
		}

		switch {
		case b.counts.TotalFailures == 0: // только успехи — продолжаем серию успехов
			cs += b.counts.ConsecutiveSuccesses
			cf = 0
		case b.counts.TotalSuccesses == 0: // только провалы — продолжаем серию провалов
			cf += b.counts.ConsecutiveFailures
			cs = 0
		default: // смешанный бакет — серия начинается заново внутри него
			cs = b.counts.ConsecutiveSuccesses
			cf = b.counts.ConsecutiveFailures
		}
	}

	total.ConsecutiveSuccesses = cs
	total.ConsecutiveFailures = cf
	return total
}

func (w *rollingWindow) clear() {
	for i := range w.buckets {
		w.buckets[i] = bucket{}
	}
}

// ReadyToTrip решает, открывать ли breaker на основе текущих счётчиков окна.
type ReadyToTrip func(Counts) bool

// Settings — конфигурация circuit breaker.
type Settings struct {
	Name string

	// MaxRequests — макс. запросов в half-open.
	MaxRequests uint32

	// Interval — полная длительность окна скольжения.
	// (в новой реализации именно окно, а не период сброса)
	Interval time.Duration

	// Buckets — количество бакетов в кольцевом буфере.
	// Ширина одного бакета = Interval / Buckets. По умолчанию 10.
	Buckets int

	// Timeout — время нахождения в open до half-open.
	Timeout time.Duration

	ReadyToTrip   ReadyToTrip
	OnStateChange func(name string, from, to State)
	IsSuccessful  func(err error) bool
}

func (s Settings) withDefaults() Settings {
	if s.MaxRequests == 0 {
		s.MaxRequests = 1
	}
	if s.Interval == 0 {
		s.Interval = 60 * time.Second
	}
	if s.Buckets == 0 {
		s.Buckets = 10
	}
	if s.Buckets < 1 {
		s.Buckets = 1
	}
	if s.Timeout == 0 {
		s.Timeout = 60 * time.Second
	}
	if s.ReadyToTrip == nil {
		s.ReadyToTrip = func(c Counts) bool { return c.ConsecutiveFailures > 5 }
	}
	if s.IsSuccessful == nil {
		s.IsSuccessful = func(err error) bool { return err == nil }
	}
	return s
}

// CircuitBreaker — generic circuit breaker со скользящим временным окном.
type CircuitBreaker[T any] struct {
	mu         sync.Mutex
	settings   Settings
	state      State
	generation uint64
	window     *rollingWindow
	expiry     time.Time
}

// NewCircuitBreaker создаёт новый CircuitBreaker.
func NewCircuitBreaker[T any](s Settings) *CircuitBreaker[T] {
	s = s.withDefaults()
	bucketWidth := s.Interval / time.Duration(s.Buckets)
	if bucketWidth <= 0 {
		bucketWidth = time.Nanosecond
	}
	cb := &CircuitBreaker[T]{
		settings: s,
		state:    StateClosed,
		window:   newRollingWindow(s.Buckets, bucketWidth),
	}
	cb.toNewGeneration(time.Now())
	return cb
}

// Name возвращает имя breaker'а.
func (cb *CircuitBreaker[T]) Name() string {
	return cb.settings.Name
}

// State возвращает текущее состояние (с учётом истечения таймеров).
func (cb *CircuitBreaker[T]) State() State {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.currentState(time.Now())
}

// Counts возвращает агрегированные счётчики текущего окна.
func (cb *CircuitBreaker[T]) Counts() Counts {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.window.aggregate(time.Now())
}

// Execute выполняет fn через breaker. Возвращает ErrOpenState / ErrTooManyRequests,
// если вызов отклонён.
func (cb *CircuitBreaker[T]) Execute(fn func() (T, error)) (T, error) {
	gen, err := cb.beforeRequest()
	if err != nil {
		var zero T
		return zero, err
	}
	result, fnErr := fn()
	cb.afterRequest(gen, cb.settings.IsSuccessful(fnErr))
	return result, fnErr
}

func (cb *CircuitBreaker[T]) beforeRequest() (uint64, error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()
	state := cb.currentState(now)
	if state == StateOpen {
		return 0, ErrOpenState
	}
	if state == StateHalfOpen && cb.window.aggregate(now).Requests >= cb.settings.MaxRequests {
		return 0, ErrTooManyRequests
	}
	cb.window.onRequest(now)
	return cb.generation, nil
}

func (cb *CircuitBreaker[T]) afterRequest(gen uint64, success bool) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	// Ответ от предыдущего поколения — игнорируем.
	if gen != cb.generation {
		return
	}

	now := time.Now()
	if success {
		cb.window.onSuccess(now)
	} else {
		cb.window.onFailure(now)
	}

	switch cb.currentState(now) {
	case StateClosed:
		if cb.settings.ReadyToTrip(cb.window.aggregate(now)) {
			cb.setState(StateOpen, now)
		}
	case StateHalfOpen:
		if cb.window.aggregate(now).ConsecutiveSuccesses >= cb.settings.MaxRequests {
			cb.setState(StateClosed, now)
		}
	}
}

// currentState лениво применяет переход open → half-open по истечении Timeout.
// Вызывается только под уже захваченным мьютексом.
func (cb *CircuitBreaker[T]) currentState(now time.Time) State {
	if cb.state == StateOpen && now.After(cb.expiry) {
		cb.setState(StateHalfOpen, now)
	}
	return cb.state
}

func (cb *CircuitBreaker[T]) setState(state State, now time.Time) {
	if cb.state == state {
		return
	}
	prev := cb.state
	cb.state = state
	cb.toNewGeneration(now)
	if cb.settings.OnStateChange != nil {
		cb.settings.OnStateChange(cb.settings.Name, prev, state)
	}
}

func (cb *CircuitBreaker[T]) toNewGeneration(now time.Time) {
	cb.generation++
	cb.window.clear()
	cb.expiry = time.Time{}
	if cb.state == StateOpen {
		cb.expiry = now.Add(cb.settings.Timeout)
	}
}
