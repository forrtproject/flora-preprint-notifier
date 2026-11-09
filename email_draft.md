Subject: {{ 'A replication attempt' if total_replication_count == 1 else 'Replication attempts' }} related to {{ 'a citation' if originals|length == 1 else 'citations' }} in your {{ server_name }} preprint

{{ author_greeting }}

Thank you for sharing *{{ preprint_title }}* openly on {{ server_name }}. Our automated system identified {{ 'a replication attempt for one of the studies' if total_replication_count == 1 else 'replication attempts for some of the studies' }} cited in your preprint in the FORRT Library of Reproduction and Replication Attempts (FLoRA).

We are part of the FORRT team maintaining FLoRA, a community-driven database that links replication attempts to the studies they examine. Because replication attempts are not consistently connected to original studies in common databases, we are sharing replication evidence identified from the references in your preprint{% if some_replications_cited %}, which you might find relevant in addition to the {{ 'replication' if cited_replication_count == 1 else 'replications' }} you are already citing{% endif %}.

{% for original in originals %}
**Cited:** {{ original.full_reference }}{% if original.doi %} [{{ original.doi }}]({{ original.doi_url }}){% endif %}
{% if original.replications|length > 1 %}

*({{ original.replications|length }} replication attempts found in FLoRA)*
{% endif %}

{% for replication in original.replications %}
> **Replication{% if original.replications|length > 1 %} {{ loop.index }}{% endif %}:** {{ replication.full_reference }}{% if replication.doi %} [{{ replication.doi }}]({{ replication.doi_url }}){% endif %}{% if replication.oa_url %} — [Open Access]({{ replication.oa_url }}){% endif %} — Reported as {{ replication.outcome }}
>
{% endfor %}

{% endfor %}

If relevant to your framing and interpretation, you may wish to consider citing or contextualising replication evidence alongside the original reference to help readers evaluate the current evidence base.

This message is informational and was generated automatically using publicly available bibliographic metadata. We are conducting a UKRI-funded study evaluating whether such notifications are useful to researchers.

**One-click feedback: Was this email helpful?**

[Helpful]({{ feedback_helpful_url }}) | [Not helpful]({{ feedback_not_helpful_url }}) | [Already aware of {{ 'this replication' if total_replication_count == 1 else 'these replications' }}]({{ feedback_already_aware_url }}) | [Report data error/concern]({{ feedback_report_error_url }})

*(You will be able to provide additional feedback after clicking. By clicking, you consent for your response to be included in our analysis.)*

If you have any questions or comments, please reply directly to this email.

Sincerely,

**The FLoRA Team**

Dr Lukas Wallrich, Birkbeck, University of London<br>
Dr Lukas Röseler, Münster Center for Open Science, University of Münster<br>
Dr Josefina Weinerova, Birkbeck, University of London<br>
Keegan Vaz, Technical University Dortmund<br>
for the FLoRA Notify Collaboration

This email is part of the UKRI-funded project *Making Replications Count*. Further information and additional discovery tools are available at: [https://forrt.org/marco](https://forrt.org/marco)

We may contact you about future preprints you publish. If you do not want to receive such emails, please [unsubscribe here]({{ unsubscribe_mailto }}).

<small>

**Data protection notice**
We use publicly accessible bibliographic information from OSF Preprints and related databases. We process only your name and email address associated with your preprint to send this message. Your data will not be shared. If you choose to provide feedback via the links above, that information will be used for research purposes. Full details are available in our [privacy notice](https://forrt.org/marco/privacy). We will send at most one notification per preprint.

</small>
