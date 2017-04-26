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
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

/**
 * Integration tests.
 */
@Test(dependsOnGroups = { "RegistrationClientIT" })
public class DiscoveryClientIT extends AbstractITBase {

    @Test
    public void testFindSubNodes() throws Exception {
        List<RegistrationClient> workers = new ArrayList<RegistrationClient>();
        workers.add(new RegistrationClient(getCurator(), basePath + "/a/b", "z", "127.0.0.1", "http:80", null)
                    .advertiseAvailability());
        workers.add(new RegistrationClient(getCurator(), basePath + "/a/b/c", "y", "127.0.0.1", "http:80", null)
                    .advertiseAvailability());
        workers.add(new RegistrationClient(getCurator(), basePath + "/a/b/c", "y", "127.0.0.2", "http:80", null)
                    .advertiseAvailability());
        
        DiscoveryClient discovery = new DiscoveryClient(getCurator(), "/http" + basePath,
        new ServiceDiscoveryManagerImpl(getCurator())
        );
        TreeMap<String, MetaData> services = new TreeMap<String, MetaData>();
        discovery.findSubNodes(services, "/http" + basePath);
        assertEquals(services.size(), 3);
        assertTrue(services.containsKey("/http" + basePath + "/a/b/z/127.0.0.1:80"));
        assertTrue(services.containsKey("/http" + basePath + "/a/b/c/y/127.0.0.1:80"));
        assertTrue(services.containsKey("/http" + basePath + "/a/b/c/y/127.0.0.2:80"));

        for (RegistrationClient worker : workers) {
            worker.deAdvertiseAvailability();
        }
    }

    @Test
    public void testFindDirectories() throws Exception {
        List<RegistrationClient> workers = new ArrayList<RegistrationClient>();
        workers.add(new RegistrationClient(getCurator(), basePath + "/a/b", "z", "127.0.0.1", "http:80", null)
                    .advertiseAvailability());
        workers.add(new RegistrationClient(getCurator(), basePath + "/a/b/c", "y", "127.0.0.1", "http:80", null)
                    .advertiseAvailability());
        ServiceDiscoveryManager manager = new ServiceDiscoveryManagerImpl(null);
        DiscoveryClient discovery = new DiscoveryClient(getCurator(), "/http" + basePath, Arrays.asList("a"),manager);

        List<String> dirs = discovery.findDirectories("/http" + basePath + "/a");
        assertEquals(dirs.size(), 1);
        assertTrue(dirs.contains("/http" + basePath + "/a/b"));

        dirs = discovery.findDirectories("/http" + basePath + "/a/b");
        assertEquals(dirs.size(), 1);
        assertTrue(dirs.contains("/http" + basePath + "/a/b/c"));

        for (RegistrationClient worker : workers) {
            worker.deAdvertiseAvailability();
        }
    }

    @Test
    public void testFindChildren() throws Exception {
        List<RegistrationClient> workers = new ArrayList<RegistrationClient>();
        workers.add(new RegistrationClient(getCurator(), basePath, "vanilla", "127.0.0.1", "http:80", null)
                    .advertiseAvailability());
        workers.add(new RegistrationClient(getCurator(), basePath, "vanilla", "127.0.0.2", "http:80", null)
                    .advertiseAvailability());
        workers.add(new RegistrationClient(getCurator(), basePath + "/x", "vanilla", "127.0.0.1", "http:80", null)
                    .advertiseAvailability());

        Map<String, MetaData> instances = new TreeMap<String, MetaData>();
        ServiceDiscoveryManager manager = new ServiceDiscoveryManagerImpl(getCurator());
        DiscoveryClient discovery = new DiscoveryClient(getCurator(), basePath, Arrays.asList("http"),manager);
        discovery.findChildren(instances, "/http" + basePath);
        assertEquals(instances.size(), 2);

        instances.clear();
        discovery.findChildren(instances, "/chocolate" + basePath);
        assertEquals(instances.size(), 0);

        instances.clear();
        discovery.findChildren(instances, basePath + "/x");
        assertEquals(instances.size(), 0);

        for (RegistrationClient worker : workers) {
            worker.deAdvertiseAvailability();
        }
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
