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
test:ok(agg:init{ cleanup_interval = 0.01 } > 0, 'First init megaagg')

local push = {}

for i = 1, 1000 do
    table.insert(push, i)
end

local started = fiber.time()
fiber.create(function()
    local list = agg:take('tube', 1000, 5)

    test:is(#list, 1000, 'tasks count')
    test:ok(fiber.time() - started < .5, 'timeout has not exceeded')
    
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
end)

fiber.sleep(0.25)
agg:push_list('tube', push, { ttl = 10 })
fiber.sleep(0.01)
-- test:diag(tnt.log())
os.exit(test:check() == true and 0 or -1)


