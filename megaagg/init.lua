local log = require 'log'
local fiber = require 'fiber'


local ID        = 1
local TUBE      = 2
local TIME      = 3
local TTL_TO    = 4
local DATA      = 5

local agg = {
    VERSION     = '1.0',
    _isAgg      = true,

    defaults = {
        ttl         = 5 * 60,       -- task ttl

        persistent  = false,        -- persistent aggregator

        -- aggregate until the limit exceeded
        limit       = 150,

        -- aggregate until the timeout exceeded
        timeout     = 1.5,
    },

    private = {
        migrations  = require('megaagg.migrations'),

        id          = nil,

        count       = {  }
    }
}

function agg._extend(self, t1, t2)
    local res = {}

    if t1 ~= nil then
        for k, v in pairs(t1) do
            res[k] = v
        end
    end

    if t2 ~= nil then
        for k, v in pairs(t2) do
            if res[k] ~= nil and v ~= nil and type(res[k]) ~= type(v) then
                box.error(box.error.PROC_LUA,
                    string.format(
                        'Wrong type for ".%s": %s (have to be %s)',
                        tostring(k),
                        type(v),
                        type(res[k])
                    )
                )
            end
            res[k] = v
        end
    end

    return res
end

function agg._next_id(self)
    if self.private.id == nil then
        self.private.id = 0
        
        local m1 = box.space.MegaAgg.index.id:max()
        local m2 = box.space.MegaAggMemOnly.index.id:max()


        if m1 ~= nil then
            if m1[ID] > self.private.id then
                self.private.id = m1[ID]
            end
        end

        if m2 ~= nil then
            if m2[ID] > self.private.id then
                self.private.id = m2[ID]
            end
        end
    end
    self.private.id = self.private.id + tonumber64(1)
    return self.private.id
end

function agg.init(self, defaults)
    if type(self) ~= 'table' or not self._isAgg then
        box.error(box.error.PROC_LUA, "usage: megaagg:init([defaults])")
    end
    self.defaults = self:_extend(self.defaults, defaults)
    local upgrades = self.private.migrations:upgrade(self)

    local spaces = { 'MegaAgg', 'MegaAggMemOnly' }
    self.private.count = {}

    log.info('MegaAgg: fill counters')
    for _, space in pairs(spaces) do
        for _, t in box.space[space].index.id:pairs() do
            local tube = t[TUBE]
            if self.private.count[tube] == nil then
                self.private.count[tube] = 1
            else
                self.private.count[tube] = self.private.count[tube] + 1
            end
        end
    end

    log.info('MegaAgg started')
    return upgrades
end

function agg.push(self, tube, data, opts)
    if type(self) ~= 'table' or not self._isAgg then
        box.error(box.error.PROC_LUA, "usage: megaagg:push(tube, data[, opts])")
    end
    opts = self:_extend(self.defaults, opts)
    local space = 'MegaAggMemOnly'
    if opts.persistent then
        space = 'MegaAgg'
    end

    local n = box.space[space]:insert {
        self:_next_id(),
        tube,
        fiber.time64(),
        fiber.time64() + tonumber64(tonumber(opts.ttl) * 1000000),
        data
    }
    if self.private.count[tube] == nil then
        self.private.count[tube] = 0
    else
        self.private.count[tube] = self.private.count[tube] + 1
    end

    self:_wakeup_waiters(tube)

    return n
end

function agg.take(self, tube, limit, timeout)

end

function agg._wakeup_waiters(self, tube)

end

local priv = {}
local pub = {}
for key, m in pairs(agg) do
    if string.match(key, '^_') then
        priv[key] = m
    else
        pub[key] = m
    end
end

setmetatable(pub, { __index = priv })
return pub
