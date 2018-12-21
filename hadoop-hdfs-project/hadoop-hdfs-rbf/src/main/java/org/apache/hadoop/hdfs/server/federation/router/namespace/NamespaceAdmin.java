package org.apache.hadoop.hdfs.server.federation.router.namespace;

import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.federation.router.Router;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeRpcServer;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.hadoop.hdfs.web.ParamFilter;
import org.apache.hadoop.hdfs.web.resources.UriFsPathParam;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;

import javax.servlet.ServletContext;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.awt.*;
import java.io.IOException;

/**
 * Manage namaspace through REST%
 */
@Path("/namespace") @ResourceFilters(ParamFilter.class) public class NamespaceAdmin {

  private @Context ServletContext context;

  protected NamenodeProtocols getRpcNamenodeProtocol() throws IOException {
    final NameNode namenode = getNameNode();
    final NamenodeProtocols np = namenode.getRpcServer();
    if (np == null) {
      throw new RetriableException("Namenode is in startup mode");
    }
    return np;
  }

  private NameNode getNameNode() {
    return (NameNode) getContext().getAttribute("name.node");
  }

  protected ServletContext getContext() {
    return context;
  }

  protected String getAbsolutePath(
      @PathParam(UriFsPathParam.NAME) final UriFsPathParam usernamespace) {
    return usernamespace.getAbsolutePath();
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  public Response createNamespace(
      @Context final UserGroupInformation ugi,
      @PathParam(UriFsPathParam.NAME) final UriFsPathParam usernamespace)
      throws IOException {
    NamenodeProtocols np = getRpcNamenodeProtocol();
    boolean status = np.createNamespace(getAbsolutePath(usernamespace));
    final String js = JsonUtil.toJsonString("boolean", status);
    return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
  }

  public boolean deleteNamespace(String usernamespace) throws IOException {
    return false;
  }

  public boolean moveNamespace(String usernamespace) throws IOException {
    return false;
  }
}
