
import redis

class PrefixFinder:
    def __init__(self, connect_port, name_max_length=10):
        self.client = redis.Redis(host='localhost', port=connect_port)
        #self.client = redis.Redis(host='prefix_finder_redis_1', port=6379, decode_responses=True)

        for key in self.client.keys():
            self.client.delete(key)
        self.name_max_length = name_max_length

    def __change_to_int(self, s):
        number_string = ''
        len_string = len(s)
        for i in range(0, self.name_max_length):
            if i < len_string:
                number_string += str(ord(s[i])).zfill(3)
            else:
                number_string += str(0).zfill(3)
        return int(number_string)

    def __change_to_int_version2(self, s):
        number_string = ''
        len_string = len(s)
        for i in range(0, self.name_max_length):
            if i < len_string:
                if 'a' <= s[i] <= 'z':
                    number_string += str(ord(s[i]) - ord('a')).zfill(2)
                elif 'A' <= s[i] <= 'Z':
                    number_string += str(ord(s[i]) - ord('A')).zfill(2)
                else:
                    print('error - not an ABC character')
            else:
                number_string += str(0).zfill(2)
        return int(number_string)

    def __change_to_int_max(self, s):
        number_string = ''
        len_string = len(s)
        for i in range(0, self.name_max_length):
            if i == len_string-1:
                number_string += str(ord(s[i])+1).zfill(3)
            elif i < len_string:
                number_string += str(ord(s[i])).zfill(3)
            else:
                number_string += str(0).zfill(3)
        return int(number_string)

    def __change_to_int_max_version2(self, s):
        number_string = ''
        len_string = len(s)
        for i in range(0, self.name_max_length):
            if i == len_string-1:
                if 'a' <= s[i] <= 'z':
                    number_string += str(ord(s[i]) - ord('a') + 1).zfill(2)
                elif 'A' <= s[i] <= 'Z':
                    number_string += str(ord(s[i]) - ord('A') + 1).zfill(2)
                else:
                    print('error - not an ABC character')
            elif i < len_string:
                if 'a' <= s[i] <= 'z':
                    number_string += str(ord(s[i]) - ord('a')).zfill(2)
                elif 'A' <= s[i] <= 'Z':
                    number_string += str(ord(s[i]) - ord('A')).zfill(2)
                else:
                    print('error - not an ABC character')
            else:
                number_string += str(0).zfill(2)
        return int(number_string)




    def add_name(self, name):
        # string key
        self.client.set(name, name)

        # list key
        self.client.lpush('list', name)

        # sorted set key
        if len(name) > self.name_max_length:
            return
        tmp_key = self.__change_to_int_version2(name)
        print(f'key: {tmp_key}, value: {name}')
        self.client.zadd('set', {name: tmp_key})




    def got_names(self, prefix, max_amount):

        # take it from the keys, probably will look on all the keys to find if the query is true - O(n),
        # I sort it on O(nlog(n))
        keys_string_redis = []
        for key in self.client.scan_iter(prefix+"*"):
            if key == b'set' or key == b'list':
                continue
            keys_string_redis.append(self.client.get(key).decode('utf-8'))
        keys_string_redis.sort()
        keys_string_redis = keys_string_redis[0:max_amount]
        print(f'keys_string_redis: {keys_string_redis}')

        # to sort the list redis do probably - O(nlog(n)),
        # then I remove all the values that dont have the prefix on O(n),
        # can do it on O(log(n)) with binary search of strings
        res_list = [x.decode('utf-8') for x in self.client.sort('list', alpha=True)]
        key_list_redis = [x for x in res_list if x.startswith(prefix)]
        key_list_redis = key_list_redis[0:max_amount]
        print(f'key_list_redis: {key_list_redis}')

        # sorted set option, this work only if the names added are less than 10 digits, I think its the best option.
        min = self.__change_to_int_version2(prefix)
        max = self.__change_to_int_max_version2(prefix)
        sort_set_list_redis = self.client.zrangebyscore(b'set', min=min, max=max, start=0, num=max_amount)
        print(f'sort_set_list_redis: {sort_set_list_redis}')



# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    port = 6379
    client_prefix = PrefixFinder(port)
    client_prefix.add_name("Va")
    client_prefix.add_name("vb")
    client_prefix.add_name("vca")
    client_prefix.add_name("Vca")
    client_prefix.add_name("vcava")
    client_prefix.add_name("vcavas")
    client_prefix.add_name("vda")
    client_prefix.add_name("b")
    client_prefix.add_name("b")
    client_prefix.add_name("aa")
    client_prefix.got_names("v", 4)

