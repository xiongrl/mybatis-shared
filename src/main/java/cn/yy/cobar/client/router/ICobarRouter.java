/**
 * Copyright 1999-2011 Alibaba Group
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.yy.cobar.client.router;

import cn.yy.cobar.client.router.support.IBatisRoutingFact;
import cn.yy.cobar.client.router.support.RoutingResult;

/**
 * the routing fact can be any type, for our current ibatis-based solution, it
 * can be a wrapper object of sql action and its argument.<br>
 * for other solutions, it can be other type as per different situations.<br>
 * 
 * @author fujohnwang
 * @since 1.0
 * @see IBatisRoutingFact
 */
public interface ICobarRouter<T> {
	RoutingResult doRoute(T routingFact) throws RoutingException;
}
