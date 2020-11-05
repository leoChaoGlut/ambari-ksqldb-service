# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from resource_management.core.exceptions import ExecutionFailed, ComponentIsNotRunning
from resource_management.core.resources.system import Execute
from resource_management.libraries.script.script import Script

from common import ksqldbHome, ksqldb_tar


class KsqlDB(Script):
    def install(self, env):
        ksqldbTmpDir = '/tmp/ksqldb'
        ksqldbTarTmpPath = ksqldbTmpDir + '/ksqldb.tar'

        Execute('mkdir -p {0}'.format(ksqldbTmpDir))
        Execute('mkdir -p {0}'.format(ksqldbHome))

        Execute('wget --no-check-certificate {0} -O {1}'.format(ksqldb_tar, ksqldbTarTmpPath))

        Execute('tar -xf {0} -C {1} '.format(ksqldbTarTmpPath, ksqldbHome))

        Execute('chmod +x ' + ksqldbHome + '/bin/ksql*')

        self.configure(env)

    def stop(self, env):
        Execute('cd ' + ksqldbHome + ' && bin/ksql-server-stop')

    def start(self, env):
        self.configure(self)
        Execute(
            'cd ' + ksqldbHome + ' && nohup bin/ksql-server-start etc/ksqldb/ksql-server.properties > ksqldb.out 2>&1 &')

    def status(self, env):
        try:
            Execute(
                'export KSQL_COUNT=`ps -ef |grep -v grep |grep "io.confluent.ksql.rest.server.KsqlServerMain etc/ksqldb/ksql-server.properties" | wc -l` && `if [ $KSQL_COUNT -ne 0 ];then exit 0;else exit 3;fi `'
            )
        except ExecutionFailed as ef:
            if ef.code == 3:
                raise ComponentIsNotRunning("ComponentIsNotRunning")
            else:
                raise ef

    def configure(self, env):
        from params import ksql_server
        key_val_template = '{0}={1}\n'

        with open(ksqldbHome + '/etc/ksqldb/ksql-server.properties', 'w') as f:
            for key, value in ksql_server.iteritems():
                f.write(key_val_template.format(key, value))


if __name__ == '__main__':
    KsqlDB().execute()
