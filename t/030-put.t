#!/usr/bin/env tarantool

local yaml = require 'yaml'
local test = require('tap').test()
local fiber = require 'fiber'
test:plan(7)

local tnt = require('t.tnt')
test:ok(tnt, 'tarantool loaded')
tnt.cfg{}

local agg = require 'megaagg'
test:ok(agg, 'megaagg loaded')
test:ok(agg:init() > 0, 'First init megaagg')

test:ok(box.space.MegaAgg, 'Space  MegaAgg created')
test:ok(box.space.MegaAggMemOnly, 'Space MegaAggMemOnly created')

local id
test:test('push persistent=true',
    function(test)
        test:plan(4)
    
        local res = agg:push('tube', 'data', { persistent = true })

        test:ok(res, 'pushed')
        local s = box.space.MegaAgg:get{ res[1] }
        test:ok(s, 'stored to persistent space')

        test:is(s[2], 'tube', 'tube')
        test:is(s[5], 'data', 'data')
        id = s[1]
    end
)

test:test('push persistent=false',
    function(test)
        test:plan(5)
    
        local res = agg:push('tube', 'data', { persistent = false })

        test:ok(res, 'pushed')
        local s = box.space.MegaAggMemOnly:get{ res[1] }
        test:ok(s, 'stored to persistent space')

        test:is(s[2], 'tube', 'tube')
        test:is(s[5], 'data', 'data')

        test:is(s[1], id + 1, 'id')
    end
)

-- test:diag(tnt.log())

os.exit(test:check() == true and 0 or -1)
