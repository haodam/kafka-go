package main

type Topic struct {
	topicID uint16
	mq      Queue
}

func (t *Topic) init(topicID uint16) {
	t.topicID = topicID
	t.mq.init()
}
