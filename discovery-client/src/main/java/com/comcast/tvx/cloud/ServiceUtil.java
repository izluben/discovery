/*
 * Copyright 2014 Comcast Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.comcast.tvx.cloud;

import java.util.*;

import com.comcast.tvx.cloud.auth.AuthInfo;
import com.comcast.tvx.cloud.discovery.EasyDiscoveryBuilder;

import com.google.common.base.Charsets;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.utils.EnsurePath;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceInstanceBuilder;
import org.apache.curator.x.discovery.ServiceType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;

/**
 * Provide static utility methods.
 */
public abstract class ServiceUtil {

    /**
     * Convert service specification into a map. Perform error checking.
     *
     * @param      serviceSpec  - service specification
     * @return     Map of services
     * @exception  IllegalArgumentException  is thrown if the input cannot be properly parsed.
     */
    protected static Map<String, Integer> parseServiceSpec(String serviceSpec) {
        Map<String, Integer> serviceMap = new HashMap<String, Integer>();

        if (serviceSpec != null) {
            String[] services = serviceSpec.split(",");

            if (services.length < 1) {
                throw new IllegalArgumentException("Invalid service specification: No services found.");
            }

            for (int i = 0; i < services.length; i++) {
                String service = services[i];
                int index = service.indexOf(":");

                if (index < 0) {
                    throw new IllegalArgumentException("Invalid service specification: No name or port defined.");
                }

                Integer port = Integer.valueOf(Integer.parseInt(service.substring(index + 1)));
                serviceMap.put(service.substring(0, index), port);
            }
        }

        return serviceMap;
    }

    /**
     * Gets the single instance of RegistrationClient.
     *
     * @return  single instance of RegistrationClient
     * @throws  Exception  the exception
     */
    protected static ServiceInstance<MetaData> getServiceInstance(
            String flavor,
            int servicePort,
            String serviceAddress,
            Map<String, String> parameters) throws Exception {

        ServiceInstanceBuilder<MetaData> builder = ServiceInstance.builder();

        // Address is optional.  The Curator library will automatically use the IP from the first
        // ethernet device
        String registerAddress = (serviceAddress == null) ? builder.build().getAddress() : serviceAddress;

        MetaData metadata = new MetaData(UUID.randomUUID(), registerAddress, servicePort, flavor);
        metadata.setParameters(parameters);

        builder.name(flavor).payload(metadata).id(registerAddress + ":" +
                String.valueOf(servicePort)).serviceType(ServiceType.DYNAMIC).address(registerAddress).port(servicePort);

        return builder.build();
    }

    /**
     * Gets the discovery.
     *
     * @param   basePath          - Registration path
     * @param   curatorFramework  - Curator
     * @return  the discovery
     * @throws  Exception  the exception
     */
    protected static ServiceDiscovery<MetaData> getDiscovery(String basePath, CuratorFramework curatorFramework)
        throws Exception {

        new EnsurePath(basePath).ensure(curatorFramework.getZookeeperClient());

        ServiceDiscovery<MetaData> result =
            EasyDiscoveryBuilder.builder(MetaData.class).basePath(basePath).client(curatorFramework).build();

        /*
         * be opaque and start it, client need not care about this
         */
        result.start();

        return result;
    }

    /**
     * Gets the discovery.
     *
     * @param   basePath          - Registration path
     * @param   curatorFramework  - Curator
     * @return  the discovery
     * @throws  Exception  the exception
     */
    protected static ServiceDiscovery<MetaData> getDiscovery(String basePath, final CuratorFramework curatorFramework, final AuthInfo authInfo)
            throws Exception {

        ensurePath(curatorFramework, basePath, authInfo);

        ServiceDiscovery<MetaData> result =
                EasyDiscoveryBuilder.builder(MetaData.class).basePath(basePath).client(curatorFramework).build();

        /*
         * be opaque and start it, client need not care about this
         */
        result.start();

        return result;
    }


    private static void ensurePath(CuratorFramework curatorFramework, String path, AuthInfo authInfo) throws Exception {
        if (authInfo != null) {
            new EnsurePath(path, new AclProvider(authInfo)).ensure(curatorFramework.getZookeeperClient());
        } else  {
            new EnsurePath(path).ensure(curatorFramework.getZookeeperClient());
        }
    }

    private static class AclProvider implements ACLProvider {
        private final AuthInfo authInfo;

        public AclProvider(AuthInfo authInfo) {
            this.authInfo = authInfo;
        }

        public List<ACL> getDefaultAcl() {
            if (authInfo != null) {
                return getAuthAcl();
            } else  {
                return getAllowAllAcl();
            }
        }

        public List<ACL> getAclForPath(String s) {
            return getDefaultAcl();
        }

        private List<ACL> getAllowAllAcl() {
            return Arrays.asList(new ACL(ZooDefs.Perms.ALL, ZooDefs.Ids.ANYONE_ID_UNSAFE));
        }

        private List<ACL> getAuthAcl() {
            List<ACL> result = new ArrayList<ACL>();
            boolean allowNotAuthenticated = authInfo.getAllowNotAuthenticated();
            String authId = authInfo.getAuthId() != null ? authInfo.getAuthId() : "";
            String authScheme = authInfo.getAuthScheme() != null ? authInfo.getAuthScheme() : "";

            result.add(new ACL(ZooDefs.Perms.ALL, new Id(authScheme, authId)));

            if (allowNotAuthenticated) {
                result.addAll(getAllowAllAcl());
            }

            return result;
        }
    }

}
