#!/bin/bash
export SOL_USER=heinz1 #username to authenticate to Solace Message Broker
export SOL_VPN=heinzvpn #VPN where the client resides
export SOL_PASSWORD=heinz1 #password to authenticate to Solace Message Broker
export SOL_TOPIC=bank/data/xml #Topic or Quse for thr Bank XML messages sent to Solace Message Broker
export SOL_QUEUE=restQ
export SOL_HOST=160.101.136.65 #host IP of Solace Message Broker
export SOL_TOPIC_RESEND=bank/data/json


# Use this file to test the Scaled XML to JSON converter. THese are the variables used in OpenShift in the Template to
# run the converter and should be exposed as OpenShift Tempalte parameters

