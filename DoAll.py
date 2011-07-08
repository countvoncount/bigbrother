#!/usr/bin/env python
## -*- coding: utf-8 -*-
import os,sys
SOPHIE_BASE = os.environ.get('VURVE_SOPHIE_BASE','/Users/tiago/vurve/code/sophie');
sys.path.append(SOPHIE_BASE)

#call click churn
import sophie.lib.bigbrother.clickchurn as clickchurn
import sophie.lib.bigbrother.histograms as histograms

clickchurn.ChurnClicksToToday()
histograms.GenerateDailyHistogramsUpToToday()
histograms.GenerateDailyHistogramsUpToToday(qualified=True)
