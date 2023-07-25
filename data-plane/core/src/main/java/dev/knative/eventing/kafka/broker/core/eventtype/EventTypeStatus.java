package dev.knative.eventing.kafka.broker.core.eventtype;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.List;
import java.util.Objects;

@JsonDeserialize
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"annotations", "conditions", "observedGeneration"})
public class EventTypeStatus {
  @JsonProperty("annotations")
  private Object annotations;

  @JsonProperty("conditions")
  private List<Object> conditions;

  @JsonProperty("observedGeneration")
  private Long observerGeneration;

  public EventTypeStatus() {}

  public EventTypeStatus(Object annotations, List<Object> conditions, Long observerGeneration) {
    this.annotations = annotations;
    this.conditions = conditions;
    this.observerGeneration = observerGeneration;
  }

  @JsonProperty("annotations")
  public Object getAnnotations() {
    return this.annotations;
  }

  @JsonProperty("annotations")
  public void setAnnotations(Object annotations) {
    this.annotations = annotations;
  }

  @JsonProperty("conditions")
  public List<Object> getConditions() {
    return this.conditions;
  }

  @JsonProperty("conditions")
  public void setConditions(List<Object> conditions) {
    this.conditions = conditions;
  }

  @JsonProperty("observedGeneration")
  public Long getObserverGeneration() {
    return this.observerGeneration;
  }

  @JsonProperty("observedGeneration")
  public void setObserverGeneration(Long observerGeneration) {
    this.observerGeneration = observerGeneration;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof EventTypeStatus)) return false;
    EventTypeStatus that = (EventTypeStatus) o;
    return Objects.equals(this.getAnnotations(), that.getAnnotations()) && Objects.equals(this.getConditions(), that.getConditions()) && Objects.equals(this.getObserverGeneration(), that.getObserverGeneration());
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.getAnnotations(), this.getConditions(), this.getObserverGeneration());
  }
}
