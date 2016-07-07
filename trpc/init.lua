local trpc = {}
local method = {}

local fiber = require('fiber')
local log = require('log')
local uuid = require('uuid')

local const_status_new = 0
local const_status_working = 1

local DEFAULT_TTL = 1
local DEFAULT_TTR = 1

local ERROR_PULL_RETRY = 901
local ERROR_TIMEOUT_PULL = 902
local ERROR_TIMEOUT_WORKER = 903
local ERROR_CALL_ERROR = 904

function method.call(self, namespace, func, args, ttl, ttr)

    if namespace == nil or type(namespace) ~= 'string' then
    	log.error('call: bad argument namespace')	
    	box.error(box.error.PROC_LUA, 'Bad argument for namespace, expected string')
    end

    if func == nil or type(func) ~= 'string' then
    	log.error('call: bad argument func')
    	box.error(box.error.PROC_LUA, 'Bad argument for func, expected string')
    end	

    if ttl == nil or type(ttl) ~= 'number' then
        ttl = DEFAULT_TTL
    end        

    if ttr == nil or type(ttr) ~= 'number' then
        ttr = DEFAULT_TTR
    end  

    local id = uuid.bin()

    local resp_channel = fiber.channel(0)

    if self.namespace_channel[namespace] == nil then
        self.namespace_channel[namespace]=fiber.channel(0)
    end 

    local putdata = {id, func, args}
    local respput = self.namespace_channel[namespace]:put(putdata, ttl)

    if not respput then
        resp_channel:close()
        resp_channel = nil
        box.error{code = ERROR_TIMEOUT_PULL, reason = 'timeout pull'}
        return
    end    
    
    self.requests[id]=resp_channel
    local ret = resp_channel:get(ttr)

    resp_channel:close()
    self.requests[id] = nil

    if ret == nil and type(ret) == 'nil' then
        box.error{code = ERROR_TIMEOUT_WORKER, reason = 'timeout worker'}
        return
    end   

    if type(ret) == 'function' then

    	err, ret = ret()

    	if err ~= nil then
    		local reason = ret
    		if type(reason) ~= 'string' then
    			reason='Undefined or is not string'
    		end	

    		box.error{code = ERROR_CALL_ERROR, reason = reason}
    	end	
    end 
    
    return ret
end


function method.pull(self, namespace)
    if self.namespace_channel[namespace] == nil then
        self.namespace_channel[namespace]=fiber.channel(0)
    end

    local res = self.namespace_channel[namespace]:get(5) 

    if res == nil then
        box.error{code = ERROR_PULL_RETRY, reason = 'pull retry'}
        return
    end    

    return res
end    

function cleanupres(obj, id)
	if not obj.requests[id]:is_closed() then
		obj.requests[id]:close()
	end  
	obj.requests[id]=nil
	log.error('push: request put timeout')
end

function method.push(self, id, res)

    if self.requests[id] == nil then
        log.error('push: request not found')
        return false
    end    

    local putres = self.requests[id]:put(res, 1)

    if not putres then
        cleanupres(id)
        return false
    end    

    return true
end

function method.pusherr(self, id, res)

    local callback = function()
    	return true, res
    end

   	return self:push(id, callback)

end

function trpc.new(space, on_task_change)

    local self = setmetatable({
    	requests={},
		namespace_channel={}    	
    }, { __index = method })
    return self
end

return trpc