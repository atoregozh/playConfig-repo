package com.amazon.audhpownershipcacheremoverlambda.activity;

import static com.amazon.audhpownershipcacheremoverlambda.utils.Constants.SERVICE_NAME;

import javax.inject.Inject;
import javax.xml.bind.ValidationException;

import com.amazon.audhpownershipcacheremoverlambda.ACEMSCallback;
import com.amazon.audhpownershipcacheremoverlambda.DigitalAssetOwnershipCache;
import com.amazon.audhpownershipcacheremoverlambda.ParseSnsMessage;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;

import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;

@Log4j2
@AllArgsConstructor
public class OwnershipCacheProcessor {

    ParseSnsMessage parseSnsMessage;
    final DigitalAssetOwnershipCache digitalAssetOwnershipCache;
    final ACEMSCallback acemsCallback;


    @Inject
    public OwnershipCacheProcessor(final DigitalAssetOwnershipCache digitalAssetOwnershipCache,
                                   final ACEMSCallback acemsCallback) {
        this.parseSnsMessage = new ParseSnsMessage();
        this.digitalAssetOwnershipCache = digitalAssetOwnershipCache;
        this.acemsCallback = acemsCallback;
    }

    /**
     * process message for deletion
     * @param snsEvent event
     * @return Void
     */
    public Void process(SNSEvent snsEvent) throws Exception{
        try {
            String customerId = this.parseSnsMessage.parseCustomerId(snsEvent);
            digitalAssetOwnershipCache.deleteFromCache(customerId);

            // TODO: calls to ACEMS will depend on messages
            reportDeleteStatus("DONE");
        } catch (ValidationException e) {
            throw new ValidationException(e);
        } catch (Exception ex) {
            log.error(ex);
            reportDeleteStatus("NOT_DONE");
        }
        return null;
    }


    /**
     * Once a message is processed successfully, report the deletion
     * status to the AlexaComplianceEventManagementService
     *
     * delete status of DONE => all data purged from our system
     * delete status of NOT_DONE => encountered issue and data is not purged
     *
     * @param notification // TODO: added the notification message as a param
     * @param deleteStatus
     * @return deletion status true/false
     */
    private void reportDeleteStatus(final String deleteStatus) {
        try {
            acemsCallback.reportDeleteStatus("123831", "DELETE",
                                             SERVICE_NAME, deleteStatus);
        } catch (Exception e) {
            log.error("Failed to report deletion status", e);
        }
    }

}