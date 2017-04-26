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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;

import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

/**
 * The Class RegistrationClientIT.
 */
@Test(groups = "RegistrationClientIT")
public class RegistrationClientIT extends AbstractITBase {

    /**
     * Test simple registration.
     *
     * @throws  Exception  the exception
     */
    @Test
    public void testSimpleRegistration() throws Exception {
        RegistrationClient workerAdvertiser =
            new RegistrationClient(getCurator(), rootPath, stackPath, "vanilla", "127.0.0.1", "foo:2181", null).advertiseAvailability();

        ServiceDiscovery<MetaData> serviceDiscovery =
            ServiceDiscoveryBuilder.builder(MetaData.class).client(getCurator()).basePath(rootPath + "/foo" + stackPath).build();

        Collection<ServiceInstance<MetaData>> services = serviceDiscovery.queryForInstances("vanilla");
        log.debug("All services: " + services.toString());
        assertEquals(services.size(), 1);

        for (ServiceInstance<MetaData> worker : services) {
            assertEquals(worker.getAddress(), "127.0.0.1");
            assertEquals(worker.getPort(), new Integer(2181));
            assertEquals(worker.getName(), "vanilla");
        }

        workerAdvertiser.deAdvertiseAvailability();

        assertTrue(ServiceDiscoveryBuilder.builder(MetaData.class).client(getCurator()).basePath(rootPath).build()
                   .queryForInstances("vanilla").size() == 0);

    }

    /**
     * Test cluster registration of same type.
     *
     * @throws  Exception  the exception
     */
    @Test
    public void testClusterRegistrationOfSameType() throws Exception {
        List<RegistrationClient> workers = new ArrayList<RegistrationClient>();

        workers.add(new RegistrationClient(getCurator(), rootPath, stackPath, "POC1", "192.168.1.100", "guide:10004", null)
                    .advertiseAvailability());

        workers.add(new RegistrationClient(getCurator(), rootPath, stackPath, "POC2", "192.168.1.101", "guide:10022", null)
                    .advertiseAvailability());

        workers.add(new RegistrationClient(getCurator(), rootPath, stackPath, "POC2", "192.168.1.102", "guide:10022", null)
                    .advertiseAvailability());

        ServiceDiscovery<MetaData> serviceDiscovery =
            ServiceDiscoveryBuilder.builder(MetaData.class).client(getCurator()).basePath(rootPath + "/guide" + stackPath).build();

        Collection<String> names = serviceDiscovery.queryForNames();
        assertEquals(names.size(), 2);
        assertTrue(names.containsAll(Arrays.asList("POC1", "POC2")));

        Collection<ServiceInstance<MetaData>> poc1s = serviceDiscovery.queryForInstances("POC1");
        assertEquals(poc1s.size(), 1);

        ServiceInstance<MetaData> poc1 = poc1s.iterator().next();
        assertEquals(poc1.getName(), "POC1");
        assertEquals(poc1.getAddress(), "192.168.1.100");
        assertEquals(poc1.getPort(), new Integer(10004));

        Collection<ServiceInstance<MetaData>> poc2s = serviceDiscovery.queryForInstances("POC2");
        assertEquals(poc2s.size(), 2);

        for (RegistrationClient worker : workers) {
            worker.deAdvertiseAvailability();
        }

    }

    /**
     * Test should enforce unique name address port.
     */
    @Test(expectedExceptions = RuntimeException.class,
        expectedExceptionsMessageRegExp = "^Duplicate service being registered.*")
    public void testShouldEnforceUniqueNameAddressPort() {
        List<RegistrationClient> workers = new ArrayList<RegistrationClient>();

        workers.add(new RegistrationClient(getCurator(), rootPath, stackPath, "y", "192.168.1.101", "dayview:10022", null)
                    .advertiseAvailability());

        workers.add(new RegistrationClient(getCurator(), rootPath, stackPath, "y", "192.168.1.101", "dayview:10022", null)
                    .advertiseAvailability());

        for (RegistrationClient worker : workers) {
            worker.deAdvertiseAvailability();
        }
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testDuplicateAdvertisement() {
        RegistrationClient client = new RegistrationClient(getCurator(), rootPath, stackPath, "y", "192.168.1.101", "dayview:10022", null);
        client.advertiseAvailability();
        client.advertiseAvailability();
    }

    /**
     * Shutdown.
     */
    @AfterClass
    public void shutdown() {
        if (curatorFramework != null) {
            curatorFramework.close();
        }
    }
}
