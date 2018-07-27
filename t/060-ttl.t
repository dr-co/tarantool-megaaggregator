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
test:ok(agg:init{ cleanup_interval = 0.01 } > 0, 'First init megaagg')

for i = 1, 2000 do
    if i % 2 == 0 then 
        agg:push('short-ttl', i, { ttl = 0.1 })
    else
        agg:push('short-ttl', i, { ttl = 1 })
    end
end

test:is(agg.private.count['short-ttl'], 2000, 'tasks were put')

fiber.sleep(.2)
test:is(agg.private.count['short-ttl'], 1000, 'All tasks were cleaned')

-- test:diag(tnt.log())
os.exit(test:check() == true and 0 or -1)

