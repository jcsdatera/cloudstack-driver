package org.apache.cloudstack.storage.datastore.utils;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Type;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.BasicClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import com.cloud.utils.exception.CloudRuntimeException;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

public class DateraRestClient {
    private static final Logger s_logger = Logger.getLogger(DateraRestClient.class);

    private Gson gson = new GsonBuilder().create();
    private LoginResponse respLogin = new LoginResponse();
    private String managementIp;
 private int managementPort;
 private String userName;
 private String password;
 private static final String CONFLICT_ERROR = "ConflictError";
 private static final String AUTH_FAILED_ERROR = "AuthFailedError";
 private static final String DATERA_LOG_PREFIX = "Datera : ";
 private static final String VALIDATION_FAILED_ERROR = "ValidationFailedError";
 public static final String OP_STATE_AVAILABLE = "available";
 public static final String ACCESS_CONTROL_MODE_ALLOW_ALL="allow_all";

 public DateraRestClient(String ip, int port,String user, String pass)
 {
  managementIp = ip;
  managementPort = port;
  userName = user;
  password = pass;
  doLogin();
 }
 public List<DateraModel.InitiatorGroup> enumerateInitiatorGroupsEx()
 {
     HttpGet getRequest = new HttpGet("/v2/initiator_groups");
     setHeaders(getRequest);
     String response = execute(getRequest);
     return extractInitiatorGroups(response);
 }
 private List<DateraModel.InitiatorGroup> extractInitiatorGroups(String response)
 {
     if(null == response)
         return null;
     List<DateraModel.InitiatorGroup> initiatorGroups = new ArrayList<DateraModel.InitiatorGroup>();
     GsonBuilder gsonBuilder = new GsonBuilder();
     Type mapStringObjectType = new TypeToken<Map<String, Object>>() {}.getType();
     gsonBuilder.registerTypeAdapter(mapStringObjectType, new DateraMapKeysAdapter());
     Gson gson1 = gsonBuilder.create();

     Map<String, Object> map = gson1.fromJson(response, mapStringObjectType);
     for (Map.Entry<String, Object> entry : map.entrySet()) {
         String initiatorGroupJson = entry.getValue().toString();
         DateraModel.InitiatorGroup initiatorGroup =  gson.fromJson(initiatorGroupJson,DateraModel.InitiatorGroup.class);
         initiatorGroups.add(initiatorGroup);
      }

     return initiatorGroups;
 }

 private boolean isIopsAvailable(String appInstance, String storageInstance, String volumeName){
     String url = String.format("/v2/app_instances/%s/storage_instances/%s/volumes/%s/performance_policy", appInstance, storageInstance, volumeName);
     String response = "";
     HttpGet getRequest = new HttpGet(url);
     setHeaders(getRequest);
     response = execute(getRequest);
     //DateraModel.PerformancePolicy resp = new DateraModel.PerformancePolicy();
     DateraModel.PerformancePolicy resp = gson.fromJson(response, DateraModel.PerformancePolicy.class);
     if(resp != null && resp.totalIopsMax != 0){
        s_logger.info("DateraRestClient.isIopsAvailable : true");
        return true;
     }
     else {
        DateraModel.DateraError err = gson.fromJson(response, DateraModel.DateraError.class);
        if(err.message.contains("No data at")){
           s_logger.info("DateraRestClient.isIopsAvailable : false");
           return false;
        }
     }
     return true;
}

 public boolean updateQos(String appInstance, String storageInstance, String volumeName, long totalIOPS)
 {
     String response = "";
     String url = String.format("/v2/app_instances/%s/storage_instances/%s/volumes/%s/performance_policy", appInstance, storageInstance, volumeName);
     if (totalIOPS == 0){
         HttpDelete deleteRequest = new HttpDelete(url);
         setHeaders(deleteRequest);
         s_logger.info("Removing IOPS from datera storage");
         response = execute(deleteRequest);
     } else {
          if(!isIopsAvailable(appInstance, storageInstance, volumeName)){
              HttpPost request = new HttpPost(url);
              DateraModel.PerformancePolicy policy = new DateraModel.PerformancePolicy(totalIOPS);
              String payload = gson.toJson(policy);
              setHeaders(request);
              setPayload(request, payload);
              s_logger.info("Updating the IOPS for the storage : payload : " + payload);
              response = execute(request);
          } else {
             HttpPut request = new HttpPut(url);
             DateraModel.PerformancePolicy policy = new DateraModel.PerformancePolicy(totalIOPS);
             String payload = gson.toJson(policy);
             setHeaders(request);
             setPayload(request, payload);
             s_logger.info("Updating the IOPS for the storage : payload : " + payload);
             response = execute(request);
          }
     }
     s_logger.info(DateraUtil.LOG_PREFIX + "DateraRestClient.updateQos response : "+ response);
     DateraModel.PerformancePolicy resp = gson.fromJson(response, DateraModel.PerformancePolicy.class);

     if(resp != null && resp.totalIopsMax == totalIOPS){
         return true;
     } else if(resp == null){
         return false;
     } else {
         DateraModel.DateraError err = gson.fromJson(response, DateraModel.DateraError.class);
         if(!isIopsAvailable(appInstance, storageInstance, volumeName)){
            return true;
         }
         s_logger.error(DateraUtil.LOG_PREFIX + "Error while setting up the max IOPS " + err.message + " " + err.erros);
         if (err.message.contains("No data at")){
             throw new CloudRuntimeException("No IOPS configured");
         }
         throw new CloudRuntimeException(err.message + "\n" + err.erros);
     }
 }

 public DateraModel.AppModel getAppInstanceInfo(String appInstance)
 {
        HttpGet getRequest = new HttpGet("/v2/app_instances/"+appInstance);
        setHeaders(getRequest);
        String response = execute(getRequest);
        DateraModel.AppModel resp = gson.fromJson(response, DateraModel.AppModel.class);
        return resp;
 }


 public DateraModel.PerformancePolicy getQos(String appInstance, String storageInstance, String volumeName)
 {
    String url = String.format("/v2/app_instances/%s/storage_instances/%s/volumes/%s/performance_policy", appInstance, storageInstance, volumeName);
    HttpGet getRequest = new HttpGet(url);
    setHeaders(getRequest);

    String response = execute(getRequest);
    DateraModel.PerformancePolicy resp = gson.fromJson(response, DateraModel.PerformancePolicy.class);

    return resp;
 }
 private void setHeaders(HttpRequestBase request)
 {
     if(null != request)
     {
         request.setHeader("Content-Type","application/json");
         if(null != respLogin.getKey())
          request.setHeader("auth-token",respLogin.getKey());
     }
 }
    public List<String> enumerateInitiatorGroups()
    {
        HttpGet getRequest = new HttpGet("/v2/initiator_groups");
        setHeaders(getRequest);
        String response = execute(getRequest);
        return extractKeys(response);
    }
    public List<String> enumerateNetworkPool() {
        HttpGet getRequest = new HttpGet("/v2/access_network_ip_pools");
        setHeaders(getRequest);
        String response = execute(getRequest);
        return extractKeys(response);
    }
     private List<String> extractKeys(String response)
     {
         if(null == response)
             return null;
         List<String> keys = new ArrayList<String>();
         GsonBuilder gsonBuilder = new GsonBuilder();
         Type mapStringObjectType = new TypeToken<Map<String, Object>>() {}.getType();
         gsonBuilder.registerTypeAdapter(mapStringObjectType, new DateraMapKeysAdapter());
         Gson gson1 = gsonBuilder.create();

         Map<String, Object> map = gson1.fromJson(response, mapStringObjectType);
         for (Map.Entry<String, Object> entry : map.entrySet()) {
             keys.add(entry.getKey());
          }

         return keys;
     }

 public boolean setQos(String appInstance, String storageInstance, String volumeName, long totalIOPS)
 {
     String url = String.format("/v2/app_instances/%s/storage_instances/%s/volumes/%s/performance_policy", appInstance, storageInstance, volumeName);
     HttpPost postRequest = new HttpPost(url);
     setHeaders(postRequest);

     DateraModel.PerformancePolicy policy = new DateraModel.PerformancePolicy(totalIOPS);
     String payload = gson.toJson(policy);
     setPayload(postRequest, payload);
     String response = execute(postRequest);
     s_logger.info(DateraUtil.LOG_PREFIX + "DateraRestClient.setQos response : "+ response);
     DateraModel.PerformancePolicy resp = gson.fromJson(response, DateraModel.PerformancePolicy.class);

     if(resp != null && resp.totalIopsMax == totalIOPS){
         return true;
     } else if(resp == null){
         return false;
     } else {
         DateraModel.DateraError err = gson.fromJson(response, DateraModel.DateraError.class);
         s_logger.error(DateraUtil.LOG_PREFIX + "Error while setting up the max IOPS " + err.message + " " + err.erros);
         throw new CloudRuntimeException(err.message + "\n" + err.erros);
     }
 }
 public boolean deleteInitiatorGroup(String groupName)
 {
      HttpDelete deleteRequest = new HttpDelete("/v2/initiator_groups/"+groupName);
      setHeaders(deleteRequest);
      String response = execute(deleteRequest);
      s_logger.info("DateraRestClient.deleteInitiatorGroup response : " + response);
      DateraModel.GenericResponse resp = gson.fromJson(response, DateraModel.GenericResponse.class);

      if(null == resp) return false;
      return resp.name.equals(groupName);
 }
 public List<String> enumerateInitiatorNames()
 {
    HttpGet getRequest = new HttpGet("/v2/initiators");
    setHeaders(getRequest);
    String response = execute(getRequest);
    return extractKeys(response);
 }
 public List<String> enumerateAppInstances()
 {
     HttpGet getRequest = new HttpGet("/v2/app_instances");
     setHeaders(getRequest);
     String response = execute(getRequest);

     return extractKeys(response);
 }
   public AppInstanceInfo.VolumeInfo getVolumeInfo(String appInstance, String storageInstance, String volumeName)
   {
       String restPath = String.format("/v2/app_instances/%s/storage_instances/%s/volumes/%s", appInstance,storageInstance,volumeName);
       HttpGet getRequest = new HttpGet(restPath);
       setHeaders(getRequest);
       String response = execute(getRequest);
       AppInstanceInfo.VolumeInfo resp = gson.fromJson(response, AppInstanceInfo.VolumeInfo.class);
       return resp;
   }
   public String createNextVolume(String appInstance, String storageInstance, int volSize)
   {
        List<AppInstanceInfo.VolumeInfo> volumes = getVolumes(appInstance, storageInstance);
        String volumeName = generateNextVolumeName(volumes,"volume");
        return createVolume(appInstance, storageInstance,volumeName,volSize,3) ? volumeName : "";
   }
   public List<AppInstanceInfo.VolumeInfo> getVolumes(String appInstance, String storageInstance) {
       String restPath = String.format("/v2/app_instances/%s/storage_instances/%s/volumes", appInstance,storageInstance);
       HttpGet getRequest = new HttpGet(restPath);
       setHeaders(getRequest);
       String response = execute(getRequest);
       return getVolumeList(response);

  }
 private List<String> constructInitiatorList(List<String> initiators)
 {
     List<String> updatedList = new ArrayList<String>();
     for(String iter : initiators)
     {
         if(false == iter.contains("/initiators/"))
             updatedList.add("/initiators/"+iter);
     }
     return updatedList;
 }
 private List<String> constructInitiatorGroups(List<String> initiatorGroups)
 {
     List<String> updatedList = new ArrayList<String>();
     for(String iter : initiatorGroups)
     {
         if(false == iter.contains("/initiator_groups/"))
             updatedList.add("/initiator_groups/"+iter);
     }
     return updatedList;
 }
 public boolean updateStorageWithInitiator(String appInstance, String storageInstance, List<String> initiators, List<String> initiatorGroups)
 {
     if(null != initiators)
     {
      initiators = constructInitiatorList(initiators);
     }
     if(null != initiatorGroups)
     {
         initiatorGroups = constructInitiatorGroups(initiatorGroups);
     }
      StorageInitiator storage = new StorageInitiator(initiators,initiatorGroups);

      HttpPut putRequest = new HttpPut("/v2/app_instances/"+appInstance+"/storage_instances/"+storageInstance);
      setHeaders(putRequest);
      String payload = gson.toJson(storage);

      setPayload(putRequest, payload);

      String response = execute(putRequest);
      DateraModel.GenericResponse respObj = gson.fromJson(response, DateraModel.GenericResponse.class);
      if(null == respObj) return false;
      return respObj.name.equals(storageInstance) ? true : false;

 }
 public boolean unregisterInitiator(String iqn)
 {
    HttpDelete deleteRequest = new HttpDelete("/v2/initiators/"+iqn);
    setHeaders(deleteRequest);
    String response = execute(deleteRequest);
    s_logger.info("DateraRestClient.unregisterInitiator response ="+response);
    DateraModel.GenericResponse resp = gson.fromJson(response, DateraModel.GenericResponse.class);
    if(null == resp) return false;
    if(null == resp.id) return false;
    return resp.id.equals(iqn) ? true : false;
 }
 public boolean registerInitiator(String labelName, String iqn)
 {
    HttpPost postRequest = new HttpPost("/v2/initiators");
    setHeaders(postRequest);

    DateraModel.InitiatorModel initiator = new DateraModel.InitiatorModel(labelName, iqn);
    String payload = gson.toJson(initiator);
    setPayload(postRequest, payload);
    String response = execute(postRequest);
    s_logger.info("DateraRestClient.registerInitiator response = " + response);
    DateraModel.GenericResponse resp = gson.fromJson(response, DateraModel.GenericResponse.class);
    if(null == resp) return false;
    if(resp.name.equals(CONFLICT_ERROR))
    {
        DateraModel.DateraError error = gson.fromJson(response, DateraModel.DateraError.class);
        //the iqn already exists, no need to panic
        return true;
    }
    return resp.name.equals(labelName) ? true : false;
 }
   public boolean isAppInstanceExists(String appName)
   {
      HttpGet getRequest = new HttpGet("/v2/app_instances/"+appName);
      setHeaders(getRequest);
      String response = execute(getRequest);
      DateraModel.GenericResponse resp = gson.fromJson(response, DateraModel.GenericResponse.class);
      if(null == resp) return false;
      return resp.name.equals(appName) ? true : false;
   }

 public boolean createVolume(String appName, String storageInstance, String volName, int volSize, int replica)
 {
     HttpPost postRequest = new HttpPost("/v2/app_instances/"+appName+"/storage_instances/"+storageInstance+"/volumes");
     setHeaders(postRequest);
     DateraModel.VolumeModel vol = new DateraModel.VolumeModel(volName,volSize,replica);
     String payload = gson.toJson(vol);
     setPayload(postRequest,payload);
     String response = execute(postRequest);
     s_logger.info(DateraUtil.LOG_PREFIX+ "DateraRestClient.createVolume payload : " + payload);
     DateraModel.GenericResponse resp = gson.fromJson(response, DateraModel.GenericResponse.class);
     if(null == resp) return false;
     return resp.name.equals(volName) ? true : false;
 }
 public boolean createStorageInstance(String appName, String storageInstance, String networkPoolName)
 {
    HttpPost postRequest = new HttpPost("/v2/app_instances/"+appName+"/storage_instances");
    setHeaders(postRequest);
    networkPoolName = "/access_network_ip_pools/"+networkPoolName;
    DateraModel.StorageModelEx storage = new DateraModel.StorageModelEx(storageInstance,networkPoolName);
    String payload = gson.toJson(storage);
    setPayload(postRequest,payload);
    String response = execute(postRequest);
    DateraModel.GenericResponse resp = gson.fromJson(response, DateraModel.GenericResponse.class);
    if(null == resp) return false;
    return resp.name.equals(storageInstance) ? true : false;
 }
 public boolean createAppInstance(String appName)
 {
    HttpPost postRequest = new HttpPost("/v2/app_instances");
    setHeaders(postRequest);
    DateraModel.AppModelEx app = new DateraModel.AppModelEx(appName);
    String payload = gson.toJson(app);
    setPayload(postRequest,payload);
    String response = execute(postRequest);
    DateraModel.GenericResponse resp = gson.fromJson(response, DateraModel.GenericResponse.class);
    if(null == resp) return false;
    return resp.name.equals(appName) ? true : false;
 }
 public boolean setAdminState(String appInstance,boolean online)
 {
     DateraModel.AdminPrivilege prev = null;
     if(online)
      {
         prev = new DateraModel.AdminPrivilege("online");
      }
     else
     {
         prev =  new DateraModel.AdminPrivilege("offline",true);
     }

  HttpPut putRequest = new HttpPut("/v2/app_instances/"+appInstance);
  setHeaders(putRequest);
  String payload = gson.toJson(prev);

   setPayload(putRequest, payload);

  String response = execute(putRequest);
  DateraModel.GenericResponse respObj = gson.fromJson(response, DateraModel.GenericResponse.class);
  if(null == respObj) return false;
  return respObj.name.equals(appInstance) ? true : false;
 }
private void setPayload(HttpPut request, String payload) {
    try {
         StringEntity params = new StringEntity(payload);
         request.setEntity(params);
        } catch (UnsupportedEncodingException e) {
         e.printStackTrace();
       }
}

 public boolean resizeVolume(String appInstance, String storageInstance, String volumeInstance, int newSize)
 {
    String restPath = String.format("/v2/app_instances/%s/storage_instances/%s/volumes/%s", appInstance,storageInstance,volumeInstance);
    HttpPut putRequest = new HttpPut(restPath);
    setHeaders(putRequest);

    DateraModel.VolumeResize vol = new DateraModel.VolumeResize(newSize);
    String payload = gson.toJson(vol);

    setPayload(putRequest, payload);
    String response = execute(putRequest);
    s_logger.info(DateraUtil.LOG_PREFIX + "DateraRestClient.resizeVolume response : " + response);
    DateraModel.GenericResponse resp = gson.fromJson(response,DateraModel.GenericResponse.class);
    if(null == resp) return false;
    return resp.name.equals(volumeInstance) ? true : false;
 }

 public boolean deleteAppInstance(String appInstance)
 {
  HttpDelete deleteRequest = new HttpDelete("/v2/app_instances/"+appInstance);
  setHeaders(deleteRequest);
  String response = execute(deleteRequest);
  s_logger.info(DateraUtil.LOG_PREFIX + "DateraRestClient.deleteAppInstance response : " + response);
  DateraModel.GenericResponse respObj = gson.fromJson(response, DateraModel.GenericResponse.class);
  if(null == respObj) return false;
  return respObj.name.equals(appInstance) ? true : false;
 }
 public boolean deleteVolume(String appInstance, String storageInstance, String volumeInstance)
 {
  String restPath = String.format("/v2/app_instances/%s/storage_instances/%s/volumes/%s", appInstance,storageInstance,volumeInstance);

  HttpDelete deleteRequest = new HttpDelete(restPath);
  setHeaders(deleteRequest);
  String response = execute(deleteRequest);
  DateraModel.GenericResponse respObj = gson.fromJson(response, DateraModel.GenericResponse.class);
  if(null == respObj) return false;
  return respObj.name.equals(volumeInstance) ? true : false;

 }
 public AppInstanceInfo.StorageInstance getStorageInfo(String appInstance, String storageInstance)
 {
  String restPath = String.format("/v2/app_instances/%s/storage_instances/%s", appInstance,storageInstance);
  HttpGet getRequest = new HttpGet(restPath);
  setHeaders(getRequest);
        String response = execute(getRequest);

        AppInstanceInfo.StorageInstance storageInfo = gson.fromJson(response, AppInstanceInfo.StorageInstance.class);

        return storageInfo;
 }

 public boolean createInitiatorGroup(String groupName, List<String> initiators)
 {
  if(null != initiators)
  {
   initiators = constructInitiatorList(initiators);
  }
  HttpPost postRequest = new HttpPost("/v2/initiator_groups");
  setHeaders(postRequest);

  DateraModel.InitiatorGroup intrGroup = new DateraModel.InitiatorGroup(groupName,initiators);
  String payload = gson.toJson(intrGroup);
  s_logger.info(DateraUtil.LOG_PREFIX + "DateraRestClient.createInitiatorGroup payload ="+ payload);
  setPayload(postRequest, payload);
  String response = execute(postRequest);

  DateraModel.GenericResponse resp = gson.fromJson(response, DateraModel.GenericResponse.class);
  if(null == resp) return false;
  if(false == resp.name.equals(groupName))
  {
    DateraModel.DateraError err = gson.fromJson(response, DateraModel.DateraError.class);
    return false;
  }
  return true;
 }
private void setPayload(HttpPost request, String payload) {
   try {
         StringEntity params = new StringEntity(payload);
          request.setEntity(params);
        } catch (UnsupportedEncodingException e) {

       e.printStackTrace();
       }
}
public List<String> registerInitiators(Map<String,String> initiators)
{
    List<String> registered = new ArrayList<String>();
    List<String> existing = getInitiators();
    for(Map.Entry<String,String> iter : initiators.entrySet())
    {
        if(false == existing.contains(iter.getValue()))
        {
            if(registerInitiator(iter.getKey(), iter.getValue()))
            {
                registered.add(iter.getValue());
            }
        }
    }
    return registered;
}

 public List<String> registerInitiators(List<String> initiators)
 {
     List<String> registered = new ArrayList<String>();
     List<String> existing = getInitiators();
     for(String iter : initiators)
     {
         if(false == existing.contains(iter))
         {
             if(registerInitiator("lbl_"+UUID.randomUUID(), iter))
             {
                 registered.add(iter);
             }
         }
     }
     return registered;
 }
 public List<String> getInitiators()
 {
       HttpGet getRequest = new HttpGet("/v2/initiators");
       getRequest.setHeader("auth-token",respLogin.getKey());
       String response = execute(getRequest);
       return extractKeys(response);
 }
 public AppInstanceInfo createVolume(String appInstanceName, List<String> initiators, List<String> initiatorGroups,int volumeGB, int volReplica, String accessControlMode, String networkPoolName)
 {
     if(null != initiators)
     {
         initiators = constructInitiatorList(initiators);
     }
     if(null != initiatorGroups)
     {
         initiatorGroups = constructInitiatorGroups(initiatorGroups);
     }

       HttpPost postRequest = new HttpPost("/v2/app_instances");
       setHeaders(postRequest);

       networkPoolName = "/access_network_ip_pools/"+networkPoolName;
       String payload = generateVolumePayload(appInstanceName,initiators,initiatorGroups,volumeGB,volReplica,accessControlMode,networkPoolName);

       s_logger.info(DateraUtil.LOG_PREFIX+ "DateraRestClient.createVolume payload ="+ payload);
       setPayload(postRequest, payload);

       String response = execute(postRequest);

       s_logger.info(DateraUtil.LOG_PREFIX + "DateraRestClient.createVolume response = " + response);
       AppInstanceInfo resp = gson.fromJson(response, AppInstanceInfo.class);

       return resp;
 }

 public String generateVolumePayload(String appInstanceName,
   List<String> initiators, List<String> initiatorGroups, int volumeGB, int volReplica, String accessControlMode, String networkPoolName) {

  String payload = "";

  DateraModel.AppModel app = new DateraModel.AppModel(appInstanceName,accessControlMode,
    new DateraModel.StorageInstanceModel(
      new DateraModel.StorageModel(networkPoolName,
        new DateraModel.VolumeInstanceModel(
          new DateraModel.VolumeModel(DateraModel.defaultVolumeName, volumeGB, volReplica)),new DateraModel.ACLPolicyModel(initiators,initiatorGroups))));

  payload = gson.toJson(app);

  //System.out.println(payload);
  return payload;
 }
 private void doLogin()
 {
     HttpPut postRequest = new HttpPut("/v2/login");
     postRequest.setHeader("Content-Type","application/json");
     DateraModel.LoginModel loginInfo = new DateraModel.LoginModel();
     loginInfo.name = userName;
     loginInfo.password = password;

  try {
   StringEntity params = new StringEntity(gson.toJson(loginInfo));
        postRequest.setEntity(params);
  } catch (UnsupportedEncodingException e) {

   e.printStackTrace();
  }

  String resp = execute(postRequest);

  if(null == resp || resp.isEmpty())
  {
     //throw new RuntimeException(DATERA_LOG_PREFIX + "No response from the datera node");
     throw new RuntimeException(DATERA_LOG_PREFIX + "Logging into the Datera cluster failed, Please check the " +
          "credentials provided in URL");
  }
  DateraModel.DateraError error = gson.fromJson(resp, DateraModel.DateraError.class);
  if(null != error.name && error.name.equals(AUTH_FAILED_ERROR))
  {
    throw new RuntimeException(DATERA_LOG_PREFIX+"Authentication failure, "+error.message);
  }
  s_logger.info("DateraRestClient.doLogin succeeded");

   respLogin = gson.fromJson(resp, LoginResponse.class);

  if(null == respLogin || null == respLogin.getKey())
  {
     throw new CloudRuntimeException("Could not login to datera node");
  }
  ////System.out.println("The session key :"+respLogin.getKey());

 }
 private String execute(HttpRequest request)
 {
  DefaultHttpClient httpclient = new DefaultHttpClient();
  String resp = "";
 try {

      httpclient = getHttpClient(managementPort);
      if(null == httpclient)
      {
          throw new RuntimeException("Could not load SSL certificates while connecting to datera management server");
      }

      AuthScope authScope = new AuthScope(managementIp, managementPort, AuthScope.ANY_SCHEME);
      UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(userName, password);

      httpclient.getCredentialsProvider().setCredentials(authScope, credentials);

      // specify the host, protocol, and port
      HttpHost target = new HttpHost(managementIp, managementPort, "https");

      // specify the get request
      //System.out.println("executing request to " + target);

      HttpResponse httpResponse = httpclient.execute(target, request);
      HttpEntity entity = httpResponse.getEntity();

      //System.out.println("----------------------------------------");
      //System.out.println(httpResponse.getStatusLine());
      Header[] headers = httpResponse.getAllHeaders();
      for (int i = 0; i < headers.length; i++) {
        //System.out.println(headers[i]);
      }
      //System.out.println("----------------------------------------");

      resp = EntityUtils.toString(entity);

      if (entity != null) {
        //System.out.println(resp);
      }

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // When HttpClient instance is no longer needed,
      // shut down the connection manager to ensure
      // immediate deallocation of all system resources
      httpclient.getConnectionManager().shutdown();
    }

 return resp;
 }

    private DefaultHttpClient getHttpClient(int iPort) {
        try {
            SSLContext sslContext = SSLContext.getInstance("TLSv1"); //SSLUtils.getSSLContext();
            X509TrustManager tm = new X509TrustManager() {
                @Override
                public void checkClientTrusted(X509Certificate[] xcs, String string) throws CertificateException {
                }

                @Override
                public void checkServerTrusted(X509Certificate[] xcs, String string) throws CertificateException {
                }

                @Override
                public X509Certificate[] getAcceptedIssuers() {
                    return null;
                }
            };

            sslContext.init(null, new TrustManager[] {tm}, new SecureRandom());

            SSLSocketFactory socketFactory = new SSLSocketFactory(sslContext, SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
            SchemeRegistry registry = new SchemeRegistry();

            registry.register(new Scheme("https", iPort, socketFactory));

            BasicClientConnectionManager mgr = new BasicClientConnectionManager(registry);
            DefaultHttpClient client = new DefaultHttpClient();

            return new DefaultHttpClient(mgr, client.getParams());
        } catch (NoSuchAlgorithmException ex) {
            //throw new CloudRuntimeException(ex.getMessage());
        } catch (KeyManagementException ex) {
            //throw new CloudRuntimeException(ex.getMessage());
        }
        return null;
    }

    private List<AppInstanceInfo.VolumeInfo> getVolumeList(String volumesJson)
    {
        List<AppInstanceInfo.VolumeInfo> volumes = new ArrayList<AppInstanceInfo.VolumeInfo>();
        try
        {
            GsonBuilder gsonBuilder = new GsonBuilder();
            Type mapStringObjectType = new TypeToken<Map<String, Object>>() {}.getType();
            gsonBuilder.registerTypeAdapter(mapStringObjectType, new DateraMapKeysAdapter());
            Gson gson1 = gsonBuilder.create();

            Map<String, Object> map = gson1.fromJson(volumesJson, mapStringObjectType);
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                String volJson = entry.getValue().toString();
                AppInstanceInfo.VolumeInfo vol = gson1.fromJson(volJson, AppInstanceInfo.VolumeInfo.class);
                volumes.add(vol);
          }
        }
        catch(Exception ex)
        {
            throw new RuntimeException("Datera : unable to deserialize volume list. "+ex);
        }
       return volumes;
    }
    public String generateNextVolumeName(List<AppInstanceInfo.VolumeInfo> volumes, String filter)
    {
       List<String> names = new ArrayList<String>();
       for(AppInstanceInfo.VolumeInfo vol : volumes)
       {
           names.add(vol.name);
       }
       return generateName(names,filter);
    }
    public String generateName(List<String> nameList, String filter) {
        int maxInt = 0;
        if (nameList != null && !nameList.isEmpty()) {
            List<Integer> list=new ArrayList<>();
            /*maxInt = nameList.parallelStream().mapToInt(name -> getIntPart(name)).max().getAsInt() + 1;*/
            for (String name : nameList) {
                list.add(getIntPart(name));
            }
            Collections.sort(list);
            maxInt=list.get(list.size()-1)+1;
        }
        return filter + "-" + maxInt;
    }

    public int getIntPart(String name) {
        try {
            if (name.split("-").length > 1)
                return Integer.parseInt(name.split("-")[1]);
            return 0;
        } catch (Exception e) {
            return 0;
        }
    }
}
