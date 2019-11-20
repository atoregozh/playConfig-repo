package com.amazon.audhpownershipcacheremoverlambda;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

import javax.inject.Inject;
import javax.xml.bind.ValidationException;

import com.amazon.alexacomplianceeventmanagementservice.DeleteEventStatus;
import com.amazon.audhpownershipcacheremoverlambda.exception.AudHPOwnershipCacheRemoverException;
import com.amazon.audhpownershipcacheremoverlambda.model.ComplianceNotification;
import com.amazon.audhpownershipcacheremoverlambda.model.compliance.internal.AccountType;
import com.amazon.audhpownershipcacheremoverlambda.model.compliance.internal.AdditionalMetadata;
import com.amazon.audhpownershipcacheremoverlambda.model.compliance.internal.DataType;
import com.amazon.audhpownershipcacheremoverlambda.model.compliance.internal.NotificationObligation;
import com.amazon.audhpownershipcacheremoverlambda.model.compliance.internal.NotificationType;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.log4j.Log4j2;

@Log4j2
public class OwnershipCacheProcessor {

    private final OwnershipCacheStorageManager ownershipCacheStorageManager;
    private final ACEMSCallback acemsCallback;
    private final ObjectMapper objectMapper;

    @Inject
    public OwnershipCacheProcessor(OwnershipCacheStorageManager ownershipCacheStorageManager,
                                   ACEMSCallback acemsCallback,
                                   ObjectMapper objectMapper) {
        this.ownershipCacheStorageManager = ownershipCacheStorageManager;
        this.acemsCallback = acemsCallback;
        this.objectMapper = objectMapper;
    }

    public Void processSNS(SNSEvent snsEvent) throws AudHPOwnershipCacheRemoverException, ValidationException {

        String customerId = SnsMessageParser.parseCustomerId(snsEvent);
        ownershipCacheStorageManager.deleteFromCache(customerId);

        return null;
    }

    public Void processSQS(SQSEvent sqsEvent) {
        sqsEvent.getRecords()
            .stream()
            .map(SQSEvent.SQSMessage::getBody)
            .map(this::parseSqsMessageBody)
            .filter(Objects::nonNull)
            .forEach(this::processRequest);

        return null;
    }

    private ComplianceNotification parseSqsMessageBody(String sqsMessageBody) {
        try {
            return objectMapper.readValue(sqsMessageBody, ComplianceNotification.class);
        } catch (IOException e) {
            throw new RuntimeException("Failure during processing of sqsMessageBody", e);
        }
    }

    /**
     * see https://w.amazon.com/bin/view/WfV/Services/AudHpOwnershipCacheRemoverLambda/ for intuition behind this logic
     */
    private void processRequest(ComplianceNotification notification) {
        log.info("Handling delete notification with notificationId={}, notificationType={}", notification.getNotificationId(),
                 notification.getNotificationType());

        NotificationType notificationType = notification.getNotificationType();
        switch (notificationType) {
            case HEARTBEAT:
                reportDeleteStatus(notification, DeleteEventStatus.DONE);
                break;
            case DELETE:
                if (isChildDataDeletionRequestedByParent(notification) || !isChildRequest(notification)) {
                    reportDeleteStatus(notification, DeleteEventStatus.DONE);
                    break;
                }
                try {
                    ownershipCacheStorageManager.deleteFromCache(notification.getAccountIdentifier());
                    reportDeleteStatus(notification, DeleteEventStatus.DONE);
                } catch (Exception e) {
                    log.error("Failed to delete notification with notificationId={} from ownership cache", notification.getNotificationId(), e);
                    reportDeleteStatus(notification, DeleteEventStatus.NOT_DONE);
                }
                break;
            case PORTABILITY:
            case UNKNOWN:
            default:
                log.error("Non supported notification. notificationId={}, customerId={}, notificationType={}",
                          notification.getNotificationId(), notification.getAccountIdentifier(), notification.getNotificationType());
        }

    }


    /**
     * https://w.amazon.com/bin/view/Alexa/Privacy/AlexaPrivacyComplianceFAQ/#HWhatdoesitmeantohavedifferentAccountTypes3F
     */
    private boolean isChildRequest(ComplianceNotification notification) {
        return notification.getAdditionalMetadata()
            .flatMap(AdditionalMetadata::getAccountType)
            .filter(accountType -> accountType == AccountType.CHILD)
            .isPresent();
    }

    /**
     * per wiki, for COPPA case the only datatype included in partial deletion notifications is the CHILD_DIRECTED datatype
     * https://w.amazon.com/bin/view/Alexa/Privacy/Deletion_Orchestrator/Contract/#H2.COPPA-Deleteallandterminatecustomer2026forchildcustomerandDeletepartialandleavecustomer202628deleteonlychild-directeddata29forparentcustomer
     **/
    private boolean isChildDataDeletionRequestedByParent(ComplianceNotification notification) {
        boolean isPartialDeletion = notification.getNotificationObligation()
            .map(obligation -> obligation == NotificationObligation.DELETE_PARTIAL_AND_LEAVE_CUSTOMER_ACCOUNT_ACTIVE)
            .orElse(false);

        Set<DataType> eligibleDataTypes = notification.getEligibleDatatypes();

        return isPartialDeletion
            && eligibleDataTypes.size() == 1
            && eligibleDataTypes.contains(DataType.CHILD_DIRECTED);
    }

    private void reportDeleteStatus(ComplianceNotification notification, String deleteStatus) {
        log.info("Reporting delete status to ACEMS for notificationId={}, notificationType={}",
                 notification.getNotificationId(), notification.getNotificationType());
        acemsCallback.reportDeleteStatus(notification, deleteStatus);
    }
}