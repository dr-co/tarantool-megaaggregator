local log   = require 'log'
local json  = require 'json'
local fio   = require 'fio'

box.cfg{ listen  = os.getenv('PRIMARY_PORT'), readahead = 10240000 }

box.schema.user.create('test', { password = 'test' })
box.schema.user.grant('test', 'read,write,execute', 'universe')

local megaagg_path =
        fio.dirname(
            fio.dirname(
                fio.dirname(
                    fio.dirname(
                        fio.dirname(
                            arg[0]
                        )
                    )
                )
            )
        );


package.path =
    fio.pathjoin(megaagg_path, '?.lua;') ..
    fio.pathjoin(megaagg_path, '?/init.lua;') ..
    package.path



_G.megaagg = require('megaagg')
megaagg:init()
