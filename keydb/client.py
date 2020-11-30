from redis import *
from redis.client import Pipeline as PipelineRedis
import json

class KeyDB(StrictRedis):

    def __init__(self, **kwargs):
        super(KeyDB, self).__init__(**kwargs)
        self.keydb = self.keydb_specific()

    def expiremember(self, key, subkey, delay, unit=None):
        """
        Set timeout on a subkey. This feature only available on KeyDB
        https://docs.keydb.dev/docs/commands/#expiremember
        :param key: key added by `SADD key [subkeys]`
        :param subkey: subkey on the set
        :param delay: timeout
        :param unit: `s` or `ms`
        :return: 0 if the timeout was set, otherwise 0
        """
        args = [key, subkey, delay]
        if unit is not None and unit not in ['s', 'ms']:
            raise ValueError("`unit` must be s or ms")

        if unit:
            args.append(unit)

        return self.execute_command('EXPIREMEMBER', *args)

    def expirememberat(self, key, subkey, timestamp):
        """
        Set timeout on a subkey by timestamp instead of seconds
        https://docs.keydb.dev/docs/commands/#expirememberat
        :param key:
        :param subkey:
        :param timestamp:
        :return:
        """
        return self.execute_command('EXPIREMEMBERAT', key, subkey, timestamp)

    def nhget(self, optname, name=None):
        """
        Get data contained within a nested hash data structure
        https://docs.keydb.dev/docs/commands/#keydb.nhget
        Nested hash layer determined by name string separated by '.' delimiter
        Returns an array of arrays or json depending on first parameter
        """
        if optname == "json":
            if not name:
                raise DataError("'json' is an option specifier and cannot be used as a key name")
            resp=self.execute_command('KEYDB.NHGET', "json", name)
            return json.loads(resp)
        else:
            return self.execute_command('KEYDB.NHGET', optname)

    def nhset(self, name, *args):
        """
        https://docs.keydb.dev/docs/commands/#keydb.nhset
        Accept native KeyDB entry format or json. Multiple values can be entered indicating a list,
        or a list can also be used as a value. Key Name can either be separate or embedded in the json object
        Returns 0 for written, 1 for overwritten, or ERR
        """

        cmd_str = ""
        command_list = ()

        def traverse(value, key=None):
            nonlocal cmd_str
            nonlocal command_list
            if isinstance(value, dict):
                for k, v in value.items():
                    if len(cmd_str) == 0:
                        cmd_str = str(k) # initial path
                    else:
                        cmd_str = str(cmd_str) + '.' + str(k) # append path as we drill in
                    yield from traverse(v, k)
                    cmd_str = str(cmd_str).rsplit('.',1)[0] # reduce path as we drill out
            else:
                command_list=(cmd_str, value)
                yield key, value

        def get_cmd_queue(dict):
            nonlocal cmd_str
            nonlocal command_list
            cmd_queue=[]
            pipe_str = "nhset_pipe"
            for k, v in traverse(dict):
                cmd_queue.append(command_list)
                pipe_str = str(pipe_str) + '.nhset' + str(command_list)
            return(pipe_str)

        if type(args[0]) is not dict:
            if type(args[0]) is list:
                arg_list = args[0]
                return self.execute_command('KEYDB.NHSET', name, *arg_list)
            else:
                arg_list = args
                return self.execute_command('KEYDB.NHSET', name, *arg_list)

        if type(args[0]) is dict:
            cmd_str = name
            nhset_pipe_obj = get_cmd_queue(args[0])
            nhset_pipe = self.pipeline()
            eval(nhset_pipe_obj)
            return(nhset_pipe.execute())

    def keydb_specific(self):
        return KeyDB.keydb(self)

    class keydb:

        def __init__(self, inner):
            self.inner = inner

        def nhget(self, optname, name=None):
            """
            Get data contained within a nested hash data structure
            https://docs.keydb.dev/docs/commands/#nhget
            Nested hash layer determined by name string separated by '.' delimiter
            Returns an array of arrays or json depending on first parameter
            """
            if optname == "json":
                if not name:
                    raise DataError("'json' is an option specifier and cannot be used as a key name")
                resp=self.inner.execute_command('KEYDB.NHGET', "json", name)
                return json.loads(resp)
            else:
                return self.inner.execute_command('KEYDB.NHGET', optname)

        def nhset(self, name, *args):
            """
            Accept native KeyDB entry format or json. Multiple values can be entered indicating a list,
            or a list can also be used as a value. Key Name can either be separate or embedded in the json object
            Returns 0 for written, 1 for overwritten, or ERR
            """

            cmd_str = ""
            command_list = ()

            def traverse(value, key=None):
                nonlocal cmd_str
                nonlocal command_list
                if isinstance(value, dict):
                    for k, v in value.items():
                        if len(cmd_str) == 0:
                            cmd_str = str(k) # initial path
                        else:
                            cmd_str = str(cmd_str) + '.' + str(k) # append path as we drill in
                        yield from traverse(v, k)
                        cmd_str = str(cmd_str).rsplit('.',1)[0] # reduce path as we drill out
                else:
                    command_list=(cmd_str, value)
                    yield key, value

            def get_cmd_queue(dict):
                nonlocal cmd_str
                nonlocal command_list
                cmd_queue=[]
                pipe_str = "nhset_pipe"
                for k, v in traverse(dict):
                    cmd_queue.append(command_list)
                    pipe_str = str(pipe_str) + '.nhset' + str(command_list)
                return(pipe_str)

            if type(args[0]) is not dict:
                if type(args[0]) is list:
                    arg_list = args[0]
                    return self.inner.execute_command('KEYDB.NHSET', name, *arg_list)
                else:
                    arg_list = args
                    return self.inner.execute_command('KEYDB.NHSET', name, *arg_list)

            if type(args[0]) is dict:
                cmd_str = name
                nhset_pipe_obj = get_cmd_queue(args[0])
                nhset_pipe = self.inner.pipeline()
                eval(nhset_pipe_obj)
                return(nhset_pipe.execute())

    def pipeline(self, transaction=True, shard_hint=None):
        """
        Return a new pipeline object that can queue multiple commands for
        later execution. ``transaction`` indicates whether all commands
        should be executed atomically. Apart from making a group of operations
        atomic, pipelines are useful for reducing the back-and-forth overhead
        between the client and server.
        """
        return Pipeline(
            self.connection_pool,
            self.response_callbacks,
            transaction,
            shard_hint)


class Pipeline(KeyDB,PipelineRedis):
    def __init__(self, connection_pool, response_callbacks, transaction,
                 shard_hint):
        self.connection_pool = connection_pool
        self.connection = None
        self.response_callbacks = response_callbacks
        self.transaction = transaction
        self.shard_hint = shard_hint

        self.watching = False
        self.reset()
