local SCH_KEY       = 'MegaAgg'

local log = require 'log'
local migrations = {}
migrations.list = {
    {
        description = 'Init database',
        up  = function()
            log.info('First start of megaagg detected')
        end
    },
    {
        description = 'Create main MegaAgg space',
        up  = function()
            box.schema.space.create(
                'MegaAgg',
                {
                    engine      = 'memtx',
                    temporary   = false,
                    format  = {
                        {                           -- #1
                            ['name']    = 'id',
                            ['type']    = 'unsigned',
                        },

                        {                           -- #2
                            ['name']    = 'tube',
                            ['type']    = 'str',
                        },

                        {                           -- #3
                            ['name']    = 'time',
                            ['type']    = 'unsigned',
                        },
                        
                        {                           -- #4
                            ['name']    = 'ttl_to',
                            ['type']    = 'unsigned',
                        },

                        {                           -- #5
                            ['name']    = 'data',
                            ['type']    = '*',
                        },
                    }
                }
            )
        end
    },
    {
        description = 'Create non-persistent MegaAgg space',
        up  = function()
            box.schema.space.create(
                'MegaAggMemOnly',
                {
                    engine      = 'memtx',
                    temporary   = true,
                    format  = {
                        {                           -- #1
                            ['name']    = 'id',
                            ['type']    = 'unsigned',
                        },

                        {                           -- #2
                            ['name']    = 'tube',
                            ['type']    = 'str',
                        },

                        {                           -- #3
                            ['name']    = 'time',
                            ['type']    = 'unsigned',
                        },
                        
                        {                           -- #4
                            ['name']    = 'ttl_to',
                            ['type']    = 'unsigned',
                        },

                        {                           -- #5
                            ['name']    = 'data',
                            ['type']    = '*',
                        },
                    }
                }
            )
        end
    },
    {
        description = 'PK for MegaAgg',
        up = function()
            box.space.MegaAgg:create_index('id',
                {
                    type = 'tree',
                    unique = true,
                    parts   = { 1, 'unsigned' }
                }
            )
        end
    },
    {
        description = 'PK for MegaAggMemOnly',
        up = function()
            box.space.MegaAggMemOnly:create_index('id',
                {
                    type = 'tree',
                    unique = true,
                    parts   = { 1, 'unsigned' }
                }
            )
        end
    },
    
    {
        description = 'tube.index for MegaAgg',
        up = function()
            box.space.MegaAgg:create_index('tube',
                {
                    type = 'tree',
                    unique = false,
                    parts   = { 2, 'str', 1, 'unsigned' }
                }
            )
        end
    },
    {
        description = 'tube.index for MegaAggMemOnly',
        up = function()
            box.space.MegaAggMemOnly:create_index('tube',
                {
                    type = 'tree',
                    unique = false,
                    parts   = { 2, 'str', 1, 'unsigned' }
                }
            )
        end
    },

    {
        description = 'ttl.index for MegaAgg',
        up = function()
            box.space.MegaAgg:create_index('ttl',
                {
                    type = 'tree',
                    unique = false,
                    parts   = { 4, 'unsigned' }
                }
            )
        end
    },

    {
        description = 'ttl.index for MegaAggMemOnly',
        up = function()
            box.space.MegaAggMemOnly:create_index('ttl',
                {
                    type = 'tree',
                    unique = false,
                    parts   = { 4, 'unsigned' }
                }
            )
        end
    },
}


function migrations.upgrade(self, agg)

    local db_version = 0
    local ut = box.space._schema:get(SCH_KEY)
    local version = agg.VERSION

    if ut ~= nil then
        db_version = ut[2]
    end

    local cnt = 0
    for v, m in pairs(migrations.list) do
        if db_version < v then
            local nv = string.format('%s.%03d', version, v)
            log.info('MegaAgg: up to version %s (%s)', nv, m.description)
            m.up(agg)
            box.space._schema:replace{ SCH_KEY, v }
            agg.VERSION = nv
            cnt = cnt + 1
        end
    end
    return cnt
end


return migrations

