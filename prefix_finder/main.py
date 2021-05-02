
import redis

class PrefixFinder:
    def __init__(self, connect_port):
        self.client = redis.Redis(host='localhost', port=connect_port)
        for key in self.client.keys():
            self.client.delete(key)
        self.max_l = 0

    #not in use
    def __change_to_int(self, s):
        number_string = ''
        for c in s:
            number_string += str(ord(c)).zfill(3)
        return int(number_string)

    # not in use
    def __change_to_int_max(self, s):
        number_string = ''
        for i in range(0, len(s)):
            if i == len(s) -1:
                break
            number_string += str(ord(s[i])).zfill(3)
        if len(s) < self.max_l:
            return int(number_string) + 1
        else:
            number_string += str(ord(s[len(s) -1]) + 1).zfill(3)
            for i in range(self.max_l - len(s)):
                number_string += str(0).zfill(3)


    def add_name(self, name):
        # string key
        self.client.set(name, name)

        # list key
        self.client.lpush('list', name)

        # sorted set key
        '''
        tmp_key = self.__change_to_int(name)
        print(f'f key: {tmp_key}, value: {name}')
        self.client.zadd('set', {name: tmp_key})
        if self.max_l < len(name):
            self.max_l = len(name)
        '''


    def got_names(self, prefix, max_amount):

        # take it from the keys, probably will look on all the keys to find if the query is true - O(n),
        # I sort it on O(nlog(n))
        keys_string_redis = []
        for key in self.client.scan_iter(prefix+"*"):
            if key == b'set' or key == b'list':
                continue
            keys_string_redis.append(self.client.get(key).decode('utf-8'))
        keys_string_redis.sort()
        print(f'keys_string_redis: {keys_string_redis}')

        # to sort the list redis do probably - O(nlog(n)),
        # then I remove all the values that dont have the prefix on O(n),
        # can do it on O(log(n)) with binary search of strings
        res_list = [x.decode('utf-8') for x in self.client.sort('list', alpha=True)]
        key_list_redis = [x for x in res_list if x.startswith(prefix)]
        key_list_redis = key_list_redis[0:max_amount]
        print(f'key_list_redis: {key_list_redis}')

        # tried to do it with sorted set but not work because the key (from string to int)
        # min = self.__change_to_int(prefix)
        # max = self.__change_to_int_max
        #                res = self.client.zrangebyscore(b'set', min=min, max=max, start=None, num=None)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    port = 6379
    client_prefix = PrefixFinder(port)
    client_prefix.add_name("va")  # 118049
    client_prefix.add_name("vb")  # 118050
    client_prefix.add_name("vc")  # 118051
    client_prefix.add_name("b")
    client_prefix.add_name("aa")
    client_prefix.got_names("v", 2)

