#!/usr/bin/env tarantool

local yaml = require 'yaml'
local test = require('tap').test()
local fiber = require 'fiber'
test:plan(5)

local tnt = require('t.tnt')
test:ok(tnt, 'tarantool loaded')
tnt.cfg{}

local agg = require 'megaagg'
test:ok(agg, 'megaagg loaded')
test:ok(agg:init() > 0, 'First init megaagg')

test:ok(box.space.MegaAgg, 'Space  MegaAgg created')
test:ok(box.space.MegaAggMemOnly, 'Space MegaAggMemOnly created')

-- test:diag(tnt.log())

os.exit(test:check() == true and 0 or -1)
