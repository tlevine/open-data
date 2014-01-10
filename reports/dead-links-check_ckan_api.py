#!/usr/bin/env python
import json
import ckanapi
c = ckanapi.RemoteCKAN('http://datahub.io')
p = c.action.package_show(id = 'socrata-metrics-api-publicness')
print(json.dumps(p, indent = 2, separators = (',',': ')))
