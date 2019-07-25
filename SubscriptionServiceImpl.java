package com.lb.provisioning.service;

import java.util.Date;
import java.util.Optional;
import java.util.regex.Pattern;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.sleuth.SpanAccessor;
import org.springframework.stereotype.Service;

import com.lib.exception.commons.exception.DefinedErrorException;
import com.lib.uamlib.service.pojo.UserStatus;
import com.lib.useridentitylib.service.pojo.UserAccountStatusResponse;
import com.lib.useridentitylib.service.pojo.UserProfile;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class SubscriptionServiceImpl implements SubscriptionService {

    @Autowired
    private UserMsisdnModelService userMsisdnService;

    @Autowired
    private UnsubscribeModelService unsubscribeModelService;

    @Autowired
    private OperatorPropertiesConfiguration opConfiguration;

    @Autowired
    private TaskExecutor taskExecutor;

    @Autowired
    private UserProfileHelper userProfileHelper;

    @Autowired
    private ClientService client;

    @Autowired
    private SpanAccessor tracer;

    @Autowired
    private NotificationHelper helper;

    @Autowired
    private PropertiesConfiguration config;

    @Override
    public SubscriptionResponse updateSubscription(String httpMethod, String httpContextPath,
            UpdateSubscriptionRequest updateSubscriptionRequest) {
        SubscriptionResponse response = null;
        try {
            String uniqueSharedId = updateSubscriptionRequest.getUniqueSharedId();
            Boolean isSubscriptionRenewed = BooleanUtils
                    .toBoolean(updateSubscriptionRequest.getSubscriptionRenewed());
            String reason = updateSubscriptionRequest.getReason();

            log.debug("Finding UserMsisdn for UniqueSharedId [{}]", uniqueSharedId);
            Optional<UserMsisdn> optionalUserMsisdn = userMsisdnService
                    .findByUniqueSharedId(uniqueSharedId);
            if (!optionalUserMsisdn.isPresent()) {
                throw new DefinedErrorException(ErrorEnum.USER_NOT_FOUND);
            }
            UserMsisdn userMsisdn = optionalUserMsisdn.get();
            NotificationRequest notificationRequest = null;

            String userId = optionalUserMsisdn.get().getUserId();

            if (isSubscriptionRenewed) {
                notificationRequest = generateNotificationRequest(updateSubscriptionRequest,
                        userMsisdn.getMsisdn(), NotificationType.ACTIVATION, userId);
            } else {

                if (reason != null && MapUtils.isNotEmpty(config.getActions())) {
                    Pattern pattern = config.getActions()
                            .get(UserStatus.SELF_DEACTIVATED.getValue());
                    Boolean isMatch = pattern.matcher(reason).matches();
                    if (isMatch) {
                        notificationRequest = generateNotificationRequest(updateSubscriptionRequest,
                                userMsisdn.getMsisdn(), NotificationType.SELF_DEACTIVATION, userId);
                    } else {
                        notificationRequest = generateNotificationRequest(updateSubscriptionRequest,
                                userMsisdn.getMsisdn(), NotificationType.DISCONNECTION, userId);
                    }
                } else {
                    notificationRequest = generateNotificationRequest(updateSubscriptionRequest,
                            userMsisdn.getMsisdn(), NotificationType.DISCONNECTION, userId);
                }
            }
            taskExecutor.processSync(httpMethod, httpContextPath, notificationRequest,
                    opConfiguration.getCarrier(), opConfiguration.getCountry(),
                    userMsisdn.getUserId(), helper.getObjectAsString(updateSubscriptionRequest));
            response = new SubscriptionResponse(ConstantsEnum.RESPONSE_OK);
        } catch (DefinedErrorException ex) {
            String reason = "";
            reason = ex.getErrorEnum().toString();
            response = new SubscriptionResponse(ConstantsEnum.FAILED, reason);
        } catch (Exception ex) {
            String reason = "";
            reason = ex.getMessage();
            response = new SubscriptionResponse(ConstantsEnum.FAILED, reason);
        }

        return response;
    }

    @Override
    public SubscriptionResponse provisionSubscription(String httpMethod, String httpContextPath,
            ProvisionSubscriptionRequest request) {

        SubscriptionResponse response = null;
        Optional<UserMsisdn> entity = userMsisdnService
                .findByMsisdnAndUniqueSharedId(request.getMsisdn(), request.getUniqueSharedId());
        if (entity.isPresent()) {

            log.debug("Found the record against ID [{}] and MSISDN [{}]",
                    entity.get().getUniqueSharedId(), request.getMsisdn());
            UserProfile userProfile = userProfileHelper.checkMSISDNEligibility(request.getMsisdn());

            String userId = userProfile != null ? userProfile.getUserId() : StringUtils.EMPTY;

            response = activateUser(httpMethod, httpContextPath, userId, request);
        } else {
            throw new DefinedErrorException(ErrorEnum.UNIQUE_COMBINATION_NOT_FOUND,
                    request.getUniqueSharedId(), request.getMsisdn());
        }
        return response;
    }

    @Override
    public SubscriptionResponse unlinkAccount(String httpMethod, String httpContextPath,
            UnlinkAccountRequest request) {

        SubscriptionResponse response = null;
        try {
            Optional<UserMsisdn> optionalUserMsisdn = userMsisdnService
                    .findByUniqueSharedId(request.getUniqueSharedId());
            if (!optionalUserMsisdn.isPresent()) {
                throw new DefinedErrorException(ErrorEnum.USER_NOT_FOUND);
            }
            NotificationRequest notificationRequest = generateNotificationRequest(request,
                    optionalUserMsisdn.get().getMsisdn(), NotificationType.SELF_DEACTIVATION,
                    optionalUserMsisdn.get().getUserId());

            taskExecutor.processSync(httpMethod, httpContextPath, notificationRequest,
                    opConfiguration.getCarrier(), opConfiguration.getCountry(),
                    optionalUserMsisdn.get().getUserId(), helper.getObjectAsString(request));
            response = new SubscriptionResponse(ConstantsEnum.RESPONSE_OK);
        } catch (DefinedErrorException ex) {
            String reason = "";
            reason = ex.getErrorEnum().toString();
            response = new SubscriptionResponse(ConstantsEnum.FAILED, reason);
        } catch (Exception ex) {
            String reason = "";
            reason = ex.getMessage();
            response = new SubscriptionResponse(ConstantsEnum.FAILED, reason);
        }

        return response;

    }

    private SubscriptionResponse activateUser(String httpMethod, String httpContextPath,
            String userId, ProvisionSubscriptionRequest request) {
        SubscriptionResponse response = null;
        try {
            NotificationRequest notificationRequest = null;
            Optional<UserAccountStatusResponse> optUserAccountStatusResponse = userProfileHelper
                    .getUserAccountStatus(userId);

            if (optUserAccountStatusResponse.isPresent()) {
                UserAccountStatusResponse uasResponse = optUserAccountStatusResponse.get();
                UserStatus accountStatus = UserStatus
                        .valueOfIgnoreCase(uasResponse.getAccountStatus());
                log.debug("Getting User account status {}", accountStatus);
                notificationRequest = generateNotificationRequest(request, request.getMsisdn(),
                        NotificationType.ACTIVATION, userId);
            } else {
                notificationRequest = generateNotificationRequest(request, request.getMsisdn(),
                        NotificationType.ACTIVATION, userId);
            }
            notificationRequest.setLanguage(request.getLanguage());
            log.debug("Setting language :{}", notificationRequest.getLanguage());
            taskExecutor.processSync(httpMethod, httpContextPath, notificationRequest,
                    opConfiguration.getCarrier(), opConfiguration.getCountry(), userId,
                    helper.getObjectAsString(request));

            response = new SubscriptionResponse(ConstantsEnum.RESPONSE_OK);
        } catch (DefinedErrorException ex) {
            String reason = "";
            reason = ex.getErrorEnum() != null ? ex.getErrorEnum().toString() : StringUtils.EMPTY;
            response = new SubscriptionResponse(ConstantsEnum.FAILED, reason);
        } catch (Exception ex) {
            String reason = "";
            reason = ex.getMessage();
            response = new SubscriptionResponse(ConstantsEnum.FAILED, reason);
        }

        return response;

    }

    public NotificationRequest generateNotificationRequest(AbstractRequest request, String msisdn,
            NotificationType notificationType, String userId) {

        NotificationRequest notificationRequest = new NotificationRequest();

        notificationRequest.setMsisdn(msisdn);
        notificationRequest.setUniqueSharedId(request.getUniqueSharedId());
        notificationRequest.setNotificationType(notificationType);
        log.debug("opConfiguration.getBillingPeriod() {}", opConfiguration.getBillingPeriod());
        notificationRequest.setBillingPeriodType(
                BillingPeriodType.getEnum(opConfiguration.getBillingPeriod()));
        notificationRequest.setUserId(userId);

        return notificationRequest;
    }

    @Override
    public UnsubscribeResponse unsubscribe(UnsubscribeRequest unsubscribeRequest) {
        RequestsUnsubscription unsubcribeSubscription = unsubscribeModelService
                .createNewItem(unsubscribeRequest);
        ClientResponse response = null;
        ProcessedStatus status = ProcessedStatus.ERROR;

        try {

            Optional<UserMsisdn> userMsisdn = userMsisdnService
                    .findByMsisdn(unsubscribeRequest.getMsisdn());
            if (userMsisdn.isPresent()
                    && StringUtils.isNotBlank(userMsisdn.get().getUniqueSharedId())) {

                response = client.unsubscribe(unsubscribeRequest,
                        userMsisdn.get().getUniqueSharedId());
                if (response.getError() == null) {
                    status = ProcessedStatus.SUCCESS;
                }
            } else {
                throw new DefinedErrorException(ErrorEnum.INVALID_SUBSCRIBER,
                        unsubscribeRequest.getMsisdn());
            }

        } finally {
            log.info("the final Status is: {}", status);
            unsubscribeModelService.updateItem(unsubcribeSubscription, status);
        }
        return getUnsubscribeResponse(unsubscribeRequest);
    }

    private UnsubscribeResponse getUnsubscribeResponse(UnsubscribeRequest unsubscribeRequest) {
        UnsubscribeResponse response = new UnsubscribeResponse();
        response.setOperatorReferenceId(null);
        response.setMsisdn(unsubscribeRequest.getMsisdn());
        response.setUserId(unsubscribeRequest.getUserId());
        response.setCorrelationId(tracer.getCurrentSpan().traceIdString());
        response.setRequestTrxId(unsubscribeRequest.getRequestTrxId());
        response.setUnsubscriptionDate(new Date().getTime());

        return response;
    }

}
