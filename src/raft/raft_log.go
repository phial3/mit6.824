package raft

//lab2d因为涉及到日志的压缩，压缩后的日志数据的下标会发生改变，抽象一个类型统一在这里进行处理
type LogType struct {
	//下标为0依然作为哨兵节点，保存上一次snapshot的term
	Entries []LogEntry
	//lab2D,snapshot
	LastSnapshotIdx int
}

func (l *LogType) init() {
	//lab2B，这里有个关键点，log的序号是从1开始的，所以这里要填一个0来补
	l.Entries = make([]LogEntry, 0, LogInitSize)
	l.Entries = append(l.Entries, LogEntry{Term: 0, Command: 0})
	l.LastSnapshotIdx = 0
}

func (l *LogType) index(idx int) LogEntry {
	if idx < l.LastSnapshotIdx {
		panic("idx err")
	}
	return l.Entries[idx-l.LastSnapshotIdx]
}

func (l *LogType) lastIndex() int {
	return l.LastSnapshotIdx + len(l.Entries) - 1
}

func (l *LogType) trimLast(lastIdx int) {
	if lastIdx <= l.LastSnapshotIdx {
		panic("idx err")
	}
	l.Entries = l.Entries[0 : lastIdx-l.LastSnapshotIdx]
}

func (l *LogType) trimFirst(startIdx int) {
	if startIdx < l.LastSnapshotIdx {
		panic("idx err")
	}
	l.Entries = l.Entries[startIdx-l.LastSnapshotIdx:]
}

func (l *LogType) append(log LogEntry) {
	l.Entries = append(l.Entries, log)
}

func (l *LogType) slice(start int) []LogEntry {
	if start <= l.LastSnapshotIdx {
		panic("idx err")
	}
	return l.Entries[start-l.LastSnapshotIdx:]
}
