package timeline

import "time"

func (t *Timeline) backgroundLoop() {
	defer close(t.bgDone)
	if t.cfg.noBackground {
		<-t.stopCh
		return
	}
	var tick <-chan time.Time
	if t.cfg.BackgroundPeriod > 0 {
		ticker := time.NewTicker(t.cfg.BackgroundPeriod)
		defer ticker.Stop()
		tick = ticker.C
	}
	for {
		select {
		case <-t.stopCh:
			return
		case now := <-tick:
			if _, err := t.FreezeIfStale(now); err != nil {
				t.reportBackground(err)
			}
		case <-t.flushedCh:
			if t.plugins.KeySpace == nil {
				continue
			}
			if _, err := t.CreateImageLayers(t.bgCtx); err != nil {
				t.reportBackground(err)
			}
		}
	}
}

func (t *Timeline) reportBackground(err error) {
	if t.bgCtx.Err() != nil {
		return
	}
	t.setBackgroundErr(err)
}
