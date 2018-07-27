#!/usr/bin/env tarantool

local yaml = require 'yaml'
local test = require('tap').test()
local fiber = require 'fiber'
test:plan(13)

local tnt = require('t.tnt')
test:ok(tnt, 'tarantool loaded')
tnt.cfg{}

local agg = require 'megaagg'
test:ok(agg, 'megaagg loaded')
test:ok(agg:init() > 0, 'First init megaagg')

test:ok(box.space.MegaAgg, 'Space  MegaAgg created')
test:ok(box.space.MegaAggMemOnly, 'Space MegaAggMemOnly created')


for i = 1, 999 do
    if math.random() > 0.01 then
        agg:push('tube', i, { persistent =  1 })
    else
        agg:push('tube', i, { persistent =  false })
    end
end

test:is(agg.private.count.tube, 999, 'inserts')

test:ok(#box.space.MegaAgg:select{}, 'MegaAgg contains records')
test:ok(#box.space.MegaAggMemOnly:select{}, 'MegaAggMemOnly contains records')


local list
fiber.create(function()
    list = agg:take('tube', 1000, 10)
end)

fiber.sleep(.1)
test:is(agg.private.count.tube, 999, 'inserts')
agg:push('tube', 1000, { persistent =  false })
fiber.sleep(.1)
test:is(agg.private.count.tube, 0, 'inserts')
test:is(#list, 1000, 'all records were returned')

local id = 0
local ok = 0
local fail = 0
for _, t in pairs(list) do
    if t[1] > id then
        ok = ok + 1
    else
        fail = fail + 1
    end
    id = t[1]
end

test:is(ok, 1000, 'order by id')
test:is(fail, 0, 'order by id')

os.exit(test:check() == true and 0 or -1)
