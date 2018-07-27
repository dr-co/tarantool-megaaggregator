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

        count       = {  },

        waiter      = {  }
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
                if type(v) == 'number' and type(res[k]) == 'boolean' then
                    v = v ~= 0
                else
                    box.error(box.error.PROC_LUA,
                        string.format(
                            'Wrong type for ".%s": %s (have to be %s)',
                            tostring(k),
                            type(v),
                            type(res[k])
                        )
                    )
                end
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

function agg._push(self, tube, data, opts)
    
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
        self.private.count[tube] = 1
    else
        self.private.count[tube] = self.private.count[tube] + 1
    end
    return n
end

function agg.push(self, tube, data, opts)
    if type(self) ~= 'table' or not self._isAgg then
        box.error(box.error.PROC_LUA, "usage: megaagg:push(tube, data[, opts])")
    end
    tube = tostring(tube)

    box.begin()
    local n = self:_push(tube, data, opts)
    box.commit()
    self:_wakeup_waiters(tube)
    return n
end

function agg.push_list(self, tube, list, opts)
    if type(self) ~= 'table' or not(self._isAgg) or type(list) ~= 'table' then
        box.error(box.error.PROC_LUA, "usage: megaagg:push_list(tube, datalist[, opts])")
    end
    tube = tostring(tube)
    opts = self:_extend(self.defaults, opts)

    local res = {}
    box.begin()
    for _, d in pairs(list) do
        table.insert(res, self:_push(tube, d, opts))
    end

    box.commit()
    self:_wakeup_waiters(tube)

    if #res > 0 then
        if opts.need_result == nil then
            return res
        end
        if opts.need_result then
            return res
        end
    end
end

function agg._take(self, tube, limit, since)

    local m1 = box.space.MegaAgg.index.tube:min{ tube }
    local m2 = box.space.MegaAgg.index.tube:min{ tube }

    if m1 ~= nil and m1[TUBE] ~= tube then
        m1 = nil
    end
    if m2 ~= nil and m2[TUBE] ~= tube then
        m2 = nil
    end

    local has_result = false

    if m1 ~= nil and m1[TIME] <= since then
        has_result = true
    end
    if m2 ~= nil and m2[TIME] <= since then
        has_result = true
    end

    if self.private.count[tube] ~= nil then
        if self.private.count[tube] >= limit then
            has_result = true
        end
    end
    if not has_result then
        return {}
    end

    local res = {}

    local i1, p1, s1 = box.space.MegaAgg.index.tube
                            :pairs({ tube }, { iterator = 'GE' })
    local i2, p2, s2 = box.space.MegaAggMemOnly.index.tube
                            :pairs({ tube }, { iterator = 'GE' })

    local t1, t2

    s1, t1 = i1(p1, s1)
    s2, t2 = i2(p2, s2)
    while #res < limit do
        if t1 ~= nil and t1[TUBE] ~= tube then
            t1 = nil
        end
        if t2 ~= nil and t2[TUBE] ~= tube then
            t2 = nil
        end
        if t1 == nil and t2 == nil then
            break
        end
        if t1 == nil then
            table.insert(res, { 'MegaAggMemOnly', t2 })
            s2, t2 = i2(p2, s2)
        elseif t2 == nil then
            table.insert(res, { 'MegaAgg', t1 })
            s1, t1 = i1(p1, s1)
        else
            if t1[ID] < t2[ID] then
                table.insert(res, { 'MegaAgg', t1 })
                s1, t1 = i1(p1, s1)
            else
                table.insert(res, { 'MegaAggMemOnly', t2 })
                s2, t2 = i2(p2, s2)
            end
        end
    end

    if #res > 0 then
        local list = {}
        box.begin()
        for _, t in pairs(res) do
            local t = box.space[ t[1] ]:delete( t[2][ID] )
            table.insert(list, t)
        end
        box.commit()
        res = list
        self.private.count[tube] = self.private.count[tube] - #res
    end

    return res
end

function agg.take(self, tube, limit, timeout)
    if type(self) ~= 'table' or not self._isAgg then
        box.error(box.error.PROC_LUA,
            "usage: megaagg:take(tube[, limit, timeout])")
    end

    tube = tostring(tube)
    timeout = tonumber(timeout)
    limit = tonumber(limit)
    
    if timeout == nil then
        timeout = self.defaults.timeout
    end
    if limit == nil then
        limit = self.defaults.limit
    end

    local finish_time = fiber.time() + timeout

    while fiber.time() < finish_time do

        local since = fiber.time64() - tonumber64(timeout * 1000000)

        local res = self:_take(tube, limit, since)
        if #res > 0 then
            return res
        end
        local sleep = finish_time - fiber.time()
        if sleep < 0 then
            break
        end


        self:_wait_for(tube, sleep)
    end
end


function agg._wait_for(self, tube, sleep)
    local fid = fiber.self().id
    if self.private.waiter[ tube ] == nil then
        self.private.waiter[ tube ] = {}
    end
    
    self.private.waiter[ tube ][ fid ] = fiber.self()
    fiber.sleep(sleep)
    self.private.waiter[ tube ][ fid ] = nil
end

function agg._wakeup_waiters(self, tube)

    local list = self.private.waiter[ tube ]
    
    if list == nil then
        return
    end

    self.private.waiter[ tube ] = {}

    for fid, f in pairs(list) do
        f:wakeup()
    end
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
