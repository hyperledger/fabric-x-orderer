package arma

type TotalOrder interface {
	SubmitRequest(req []byte) error
	Deliver() []byte
}

type ConsensusLedger interface {
	Append(seq uint64, blockHeaders []byte)
}

type Consenter struct {
	Seq             uint64
	ConsensusLedger ConsensusLedger
	Logger          Logger
	TotalOrder      TotalOrder
}

func (c *Consenter) Run() {
	go func() {
		for {
			batch := c.TotalOrder.Deliver()
			c.ConsensusLedger.Append(c.Seq, batch)
			c.Seq++
		}
	}()
}

func (c *Consenter) Submit(rawControlEvent []byte) {
	if err := c.TotalOrder.SubmitRequest(rawControlEvent); err != nil {
		c.Logger.Warnf("Failed submitting request:", err)
		return
	}
}
