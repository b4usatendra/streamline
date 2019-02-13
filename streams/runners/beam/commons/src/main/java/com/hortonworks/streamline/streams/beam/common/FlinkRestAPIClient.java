package com.hortonworks.streamline.streams.beam.common;

import com.hortonworks.streamline.common.JsonClientUtil;
import com.hortonworks.streamline.common.exception.WrappedWebApplicationException;
import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.security.auth.Subject;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Karthik.K
 */
public class FlinkRestAPIClient {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkRestAPIClient.class);

    private static final MediaType FLINK_REST_API_MEDIA_TYPE = MediaType.APPLICATION_JSON_TYPE;
    private static final String JOBS_OVERVIEW_PATH = "/jobs/overview";
    private static final String JOB_DETAILS_PATH = "/jobs/%s";
    private static final String JOB_STOP_PATH = "/jobs/%s?mode=stop";

    private final String flinkApiRootUrl;
    private final Subject subject;
    private final Client client;

    public FlinkRestAPIClient(String flinkApiRootUrl, Subject subject, Client client) {
        this.flinkApiRootUrl = flinkApiRootUrl;
        this.subject = subject;
        this.client = client;
    }

    private Map doGetRequest(String requestUrl) {
        try {
            LOG.debug("GET request to Flink cluster: " + requestUrl);
            return Subject.doAs(subject, new PrivilegedAction<Map>() {
                @Override
                public Map run() {
                    return JsonClientUtil.getEntity(client.target(requestUrl), FLINK_REST_API_MEDIA_TYPE, Map.class);
                }
            });
        } catch (RuntimeException ex) {
            Throwable cause = ex.getCause();
            // JsonClientUtil wraps exception, so need to compare
            if (cause instanceof javax.ws.rs.ProcessingException) {
                if (ex.getCause().getCause() instanceof IOException) {
                    throw new FlinkNotReachableException("Exception while requesting " + requestUrl, ex);
                }
            } else if (cause instanceof WebApplicationException) {
                throw WrappedWebApplicationException.of((WebApplicationException) cause);
            }

            throw ex;
        }
    }

    private Map doPostRequestWithEmptyBody(String requestUrl) {
        try {
            LOG.debug("POST request to Flink cluster: " + requestUrl);
            return Subject.doAs(subject, new PrivilegedAction<Map>() {
                @Override
                public Map run() {
                    return JsonClientUtil.postForm(client.target(requestUrl), new MultivaluedHashMap<>(),
                        FLINK_REST_API_MEDIA_TYPE, Map.class);
                }
            });
        } catch (javax.ws.rs.ProcessingException e) {
            if (e.getCause() instanceof IOException) {
                throw new FlinkNotReachableException("Exception while requesting " + requestUrl, e);
            }

            throw e;
        } catch (WebApplicationException e) {
            throw WrappedWebApplicationException.of(e);
        }
    }

    private Response doPatchRequestWithEmptyBody(String requestUrl) {
        try {
            LOG.debug("POST request to Storm cluster: " + requestUrl);
            return Subject.doAs(subject, new PrivilegedAction<Response>() {
                @Override
                public Response run() {
                    return JsonClientUtil.patchForm(client.target(requestUrl), new MultivaluedHashMap<>(),
                        FLINK_REST_API_MEDIA_TYPE);
                }
            });
        } catch (javax.ws.rs.ProcessingException e) {
            if (e.getCause() instanceof IOException) {
                throw new FlinkNotReachableException("Exception while requesting " + requestUrl, e);
            }

            throw e;
        } catch (WebApplicationException e) {
            throw WrappedWebApplicationException.of(e);
        }
    }

    public Map getJobsOverview() {
        return doGetRequest(generateJobsOverviewUrl());
    }

    public Map getJobDetailsById(String jobId) {
        return doGetRequest(generateJobDetailUrl(jobId));
    }

    public Map getJobDetailsByName(String jobName) {
        HashMap<String, ArrayList<LinkedHashMap>> jobs = (HashMap<String, ArrayList<LinkedHashMap>>) getJobsOverview();
        return jobs.get("jobs").stream().filter(list -> list.get("name").toString().equalsIgnoreCase(jobName)).findFirst().orElse(null);
    }

    public String getJobIdByName(String jobName) {
        Map jobDetails = getJobDetailsByName(jobName);
        if (jobDetails == null || jobDetails.isEmpty() || jobDetails.size() == 0) {
            throw new NotFoundException("Unable to find jobId with jobName: " + jobName);
        }
        return (String) jobDetails.getOrDefault("jid", null);
    }

    public boolean stopJob(String jobId) {
        return doPatchRequestWithEmptyBody(generateJobStopUrl(jobId)).getStatus() == 202;
    }

    private String generateJobsOverviewUrl() {
        StringBuffer url = new StringBuffer(flinkApiRootUrl);
        url.append(JOBS_OVERVIEW_PATH);
        return url.toString();
    }

    private String generateJobDetailUrl(String jobId) {
        StringBuffer url = new StringBuffer(flinkApiRootUrl);
        url.append(String.format(JOB_DETAILS_PATH, jobId));
        return url.toString();
    }


    private String generateJobStopUrl(String jobId) {
        StringBuffer url = new StringBuffer(flinkApiRootUrl);
        url.append(String.format(JOB_STOP_PATH, jobId));
        return url.toString();
    }

    /*public static void main(String[] args) {
        FlinkRestAPIClient client = new FlinkRestAPIClient("http://localhost:8081",null, ClientBuilder.newClient(new ClientConfig()));
        HashMap map = (HashMap) client.getJobsOverview();
        client.getJobDetailsByName("kafka-example");
        System.out.println(map);
    }*/
}
