#ifndef LRUCACHE_H
#define	LRUCACHE_H

#include <unordered_map>
#include <list>
#include <cstddef>
#include <stdexcept>
#include "engine_services.h"

namespace cache {


// TODO multi-threaded visiting?
template<typename key_t, typename value_t>
class lru_cache {
    public:
        typedef typename std::pair<key_t, value_t> key_value_pair_t;
        typedef typename std::list<key_value_pair_t>::iterator list_iterator_t;

        lru_cache(size_t max_size) :
            _max_size(max_size) {
        }

        void put(const key_t& key, const value_t& value) {
            auto it = _cache_items_map.find(key);
	    _cache_items_list.push_front(key_value_pair_t(key, value));
            if (it != _cache_items_map.end()) {
                _cache_items_list.erase(it->second);
                _cache_items_map.erase(it);
            }
            _cache_items_map[key] = _cache_items_list.begin();

            if (_cache_items_map.size() > _max_size) {
                auto last = _cache_items_list.end();
                last--;
                _cache_items_map.erase(last->first);
                _cache_items_list.pop_back();
            }
        }

	const value_t& get(const key_t& key) {
            auto it = _cache_items_map.find(key);
            if (it == _cache_items_map.end()) {
                throw std::range_error("There is no such key in cache");
            } else {
                _cache_items_list.splice(_cache_items_list.begin(), _cache_items_list, it->second);
                return it->second->second;
            }
        }
	
        bool exists(const key_t& key) const {
            return _cache_items_map.find(key) != _cache_items_map.end();
        }
	
        size_t size() const {
            return _cache_items_map.size();
        }
	
    private:
        std::list<key_value_pair_t> _cache_items_list;
        std::unordered_map<key_t, list_iterator_t> _cache_items_map;
        size_t _max_size;
};


template<typename key_t, typename value_t> // right now value_t should be std::string TODO
class lru_flash_cache {
    public:
        lru_flash_cache(const size_t & max_size, const std::string & based_file) {
            _map_ = new lru_cache<key_t, Store_Segment>(max_size);
            _store_reader_ = new FlashReader(based_file);
        }

        ~lru_flash_cache() {
            delete _map_;
            delete _store_reader_;
        }

        void put(const key_t& key, const value_t& value) {
            // store in file   // TODO get this async 
            Store_Segment segment = _store_reader_->append(value);    //TODO what if value != string
            // update map
            _map_->put(key, segment);
        }

	//const value_t& get(const key_t& key) {
	const value_t get(const key_t& key) {
            // check map
            Store_Segment segment = _map_->get(key);
            // read from file
            std::string res = _store_reader_->read(segment);
            return res;
        }

        bool exists(const key_t& key) {
            return _map_->exists(key);
        }
        
        size_t size() {
            return _map_->size();
        }
    
    private:
        lru_cache<key_t, Store_Segment> * _map_;  // map from key to position of file
        //lru_cache<std::string, Store_Segment> * _map_;  // map from key to position of file
        FlashReader * _store_reader_;

};


} // namespace cache

#endif
