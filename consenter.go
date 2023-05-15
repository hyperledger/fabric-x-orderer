package arma

type TotalOrder interface {
	SubmitRequest(req []byte) error
	Deliver() [][]byte
}

type ConsensusLedger interface {
	Append(seq uint64, blockHeader []byte)
}

type Consenter struct {
	Seq                       uint64
	ConsensusLedger           ConsensusLedger
	Logger                    Logger
	TotalOrder                TotalOrder
	BatchAttestationFromBytes func([]byte) BatchAttestation
}

func (c *Consenter) run() {
	for {
		batch := c.TotalOrder.Deliver()
		for _, header := range batch {
			c.ConsensusLedger.Append(c.Seq, header)
			c.Seq++
		}
	}
}

func (c *Consenter) Submit(ba BatchAttestation) {
	if err := c.TotalOrder.SubmitRequest(ba.Serialize()); err != nil {
		c.Logger.Warnf("Failed submitting request:", err)
		return
	}
}
