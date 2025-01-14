/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.jdbc.hive.auth;

import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import javax.security.auth.Subject;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

/** Utility functions for HTTP mode authentication. */
public final class HttpAuthUtils {
  public static final String AUTHORIZATION = "Authorization";
  public static final String NEGOTIATE = "Negotiate";
  private static final String COOKIE_CLIENT_USER_NAME = "cu";
  private static final String COOKIE_CLIENT_RAND_NUMBER = "rn";
  private static final String COOKIE_KEY_VALUE_SEPARATOR = "=";
  private static final Set<String> COOKIE_ATTRIBUTES =
      new HashSet<String>(Arrays.asList(COOKIE_CLIENT_USER_NAME, COOKIE_CLIENT_RAND_NUMBER));

  /**
   * @return Stringified Base64 encoded kerberosAuthHeader on success
   * @throws Exception
   */
  public static String getKerberosServiceTicket(
      String principal, String host, String serverHttpUrl, Subject loggedInSubject)
      throws Exception {
    String serverPrincipal = HadoopThriftAuthBridge.getBridge().getServerPrincipal(principal, host);
    if (loggedInSubject != null) {
      return Subject.doAs(
          loggedInSubject, new HttpKerberosClientAction(serverPrincipal, serverHttpUrl));
    } else {
      // JAAS login from ticket cache to setup the client UserGroupInformation
      UserGroupInformation clientUGI =
          HadoopThriftAuthBridge.getBridge().getCurrentUGIWithConf("kerberos");
      return clientUGI.doAs(new HttpKerberosClientAction(serverPrincipal, serverHttpUrl));
    }
  }

  private HttpAuthUtils() {
    throw new UnsupportedOperationException("Can't initialize class");
  }

  /**
   * We'll create an instance of this class within a doAs block so that the client's TGT credentials
   * can be read from the Subject
   */
  public static class HttpKerberosClientAction implements PrivilegedExceptionAction<String> {
    public static final String SERVER_HTTP_URL = "SERVER_HTTP_URL";
    private final String serverPrincipal;
    private final String serverHttpUrl;
    private final Base64 base64codec;
    private final HttpContext httpContext;

    public HttpKerberosClientAction(String serverPrincipal, String serverHttpUrl) {
      this.serverPrincipal = serverPrincipal;
      this.serverHttpUrl = serverHttpUrl;
      base64codec = new Base64(0);
      httpContext = new BasicHttpContext();
      httpContext.setAttribute(SERVER_HTTP_URL, serverHttpUrl);
    }

    @Override
    public String run() throws Exception {
      // This Oid for Kerberos GSS-API mechanism.
      Oid mechOid = new Oid("1.2.840.113554.1.2.2");
      // Oid for kerberos principal name
      Oid krb5PrincipalOid = new Oid("1.2.840.113554.1.2.2.1");
      GSSManager manager = GSSManager.getInstance();
      // GSS name for server
      GSSName serverName = manager.createName(serverPrincipal, krb5PrincipalOid);
      // Create a GSSContext for authentication with the service.
      // We're passing client credentials as null since we want them to be read from the Subject.
      GSSContext gssContext =
          manager.createContext(serverName, mechOid, null, GSSContext.DEFAULT_LIFETIME);
      gssContext.requestMutualAuth(false);
      // Establish context
      byte[] inToken = new byte[0];
      byte[] outToken = gssContext.initSecContext(inToken, 0, inToken.length);
      gssContext.dispose();
      // Base64 encoded and stringified token for server
      return new String(base64codec.encode(outToken));
    }
  }
}
