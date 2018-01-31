from pyreuse.sysutils.cgroup import Cgroup

if __name__ == '__main__':
    cg = Cgroup(name='charlie', subs='memory')
    size = int(2*1024*1024*1024)
    cg.set_item('memory', 'memory.limit_in_bytes', size) #1G
    #cg.set_item('memory', 'memory.limit_in_bytes', 268435456) #256MB
    #p = cg.execute(['sudo -u kanwu -H sh -c "/users/kanwu/elasticsearch-5.6.3/bin/elasticsearch"'])
    p = cg.execute(['sudo', '-u', 'kanwu', '-H', 'sh', '-c', '''"/users/kanwu/elasticsearch-5.6.3/bin/elasticsearch"'''])
    #p = cg.execute(['ls'])
    p.wait()
