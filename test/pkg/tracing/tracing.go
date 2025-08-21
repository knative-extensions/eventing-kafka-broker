/*
 * Copyright 2022 The Knative Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tracing

// TODO(Cali0707): redo with OTel
//
//func TraceTreeMatches(sourceName, eventID string, expectedTraceTree tracinghelper.TestSpanTree) eventshub.EventInfoMatcher {
//	return func(info eventshub.EventInfo) error {
//		if err := cetest.AllOf(
//			cetest.HasSource(sourceName),
//			cetest.HasId(eventID))(*info.Event); err != nil {
//			return err
//		}
//		traceID, err := getTraceIDHeader(info)
//		if err != nil {
//			return err
//		}
//		trace, err := pkgzipkin.JSONTracePred(traceID, 5*time.Second, func(trace []model.SpanModel) bool {
//			tree, err := tracinghelper.GetTraceTree(trace)
//			if err != nil {
//				return false
//			}
//			return len(expectedTraceTree.MatchesSubtree(nil, tree)) > 0
//		})
//		if err != nil {
//			tree, err := tracinghelper.GetTraceTree(trace)
//			if err != nil {
//				return err
//			}
//			if len(expectedTraceTree.MatchesSubtree(nil, tree)) == 0 {
//				return fmt.Errorf("no matching subtree. want: %v got: %v", expectedTraceTree, tree)
//			}
//		}
//		return nil
//	}
//}
//
//// getTraceIDHeader gets the TraceID from the passed event. It returns an error
//// if trace id is not present in that message.
//func getTraceIDHeader(info eventshub.EventInfo) (string, error) {
//	if info.HTTPHeaders != nil {
//		sc := trace.SpanContextFromContext(propagation.TraceContext{}.Extract(context.TODO(), propagation.HeaderCarrier(info.HTTPHeaders)))
//		if sc.HasTraceID() {
//			return sc.TraceID().String(), nil
//		}
//	}
//	return "", fmt.Errorf("no traceid in info: (%v)", info)
//}
//
//func WithMessageIDSource(eventID, sourceName string) tracinghelper.SpanMatcherOption {
//	return func(m *tracinghelper.SpanMatcher) {
//		m.Tags = map[string]*regexp.Regexp{
//			"messaging.message_id":     regexp.MustCompile("^" + eventID + "$"),
//			"messaging.message_source": regexp.MustCompile("^" + sourceName + "$"),
//		}
//	}
//}
