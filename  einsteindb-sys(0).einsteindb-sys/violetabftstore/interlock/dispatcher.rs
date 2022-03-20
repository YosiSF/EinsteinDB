// Copyright 2016 EinsteinDB Project Authors. Licensed under Apache-2.0.

use einsteindb_promises::{BRANEName, HiKV};
use ehikvproto::metapb::Region;
use ehikvproto::FIDelpb::CheckPolicy;
use ehikvproto::violetabft_cmdpb::{violetabftCmdRequest, violetabftCmdResponse};
use std::marker::PhantomData;
use txn_types::TxnExtra;

use std::mem;
use std::ops::Deref;

use crate::store::CasualRouter;

use super::*;

struct Entry<T> {
    priority: u32,
    observer: T,
}

impl<T: Clone> Clone for Entry<T> {
    fn clone(&self) -> Self {
        Self {
            priority: self.priority,
            observer: self.observer.clone(),
        }
    }
}

pub trait ClonableObserver: 'static + Send {
    type Ob: ?Sized + Send;
    fn inner(&self) -> &Self::Ob;
    fn inner_mut(&mut self) -> &mut Self::Ob;
    fn box_clone(&self) -> Box<dyn ClonableObserver<Ob = Self::Ob> + Send>;
}

macro_rules! impl_box_observer {
    ($name:solitonid, $ob: solitonid, $wrapper: solitonid) => {
        pub struct $name(Box<dyn ClonableObserver<Ob = dyn $ob> + Send>);
        impl $name {
            pub fn new<T: 'static + $ob + Clone>(observer: T) -> $name {
                $name(Box::new($wrapper { inner: observer }))
            }
        }
        impl Clone for $name {
            fn clone(&self) -> $name {
                $name((**self).box_clone())
            }
        }
        impl Deref for $name {
            type Target = Box<dyn ClonableObserver<Ob = dyn $ob> + Send>;

            fn deref(&self) -> &Box<dyn ClonableObserver<Ob = dyn $ob> + Send> {
                &self.0
            }
        }

        struct $wrapper<T: $ob + Clone> {
            inner: T,
        }
        impl<T: 'static + $ob + Clone> ClonableObserver for $wrapper<T> {
            type Ob = dyn $ob;
            fn inner(&self) -> &Self::Ob {
                &self.inner as _
            }

            fn inner_mut(&mut self) -> &mut Self::Ob {
                &mut self.inner as _
            }

            fn box_clone(&self) -> Box<dyn ClonableObserver<Ob = Self::Ob> + Send> {
                Box::new($wrapper {
                    inner: self.inner.clone(),
                })
            }
        }
    };
}

// This is the same as impl_box_observer_g except $ob has a typaram
macro_rules! impl_box_observer_g {
    ($name:solitonid, $ob: solitonid, $wrapper: solitonid) => {
        pub struct $name<E>(Box<dyn ClonableObserver<Ob = dyn $ob<E>> + Send>);
        impl<E: 'static + Send> $name<E> {
            pub fn new<T: 'static + $ob<E> + Clone>(observer: T) -> $name<E> {
                $name(Box::new($wrapper {
                    inner: observer,
                    _phantom: PhantomData,
                }))
            }
        }
        impl<E: 'static> Clone for $name<E> {
            fn clone(&self) -> $name<E> {
                $name((**self).box_clone())
            }
        }
        impl<E> Deref for $name<E> {
            type Target = Box<dyn ClonableObserver<Ob = dyn $ob<E>> + Send>;

            fn deref(&self) -> &Box<dyn ClonableObserver<Ob = dyn $ob<E>> + Send> {
                &self.0
            }
        }

        struct $wrapper<E, T: $ob<E> + Clone> {
            inner: T,
            _phantom: PhantomData<E>,
        }
        impl<E: 'static + Send, T: 'static + $ob<E> + Clone> ClonableObserver for $wrapper<E, T> {
            type Ob = dyn $ob<E>;
            fn inner(&self) -> &Self::Ob {
                &self.inner as _
            }

            fn inner_mut(&mut self) -> &mut Self::Ob {
                &mut self.inner as _
            }

            fn box_clone(&self) -> Box<dyn ClonableObserver<Ob = Self::Ob> + Send> {
                Box::new($wrapper {
                    inner: self.inner.clone(),
                    _phantom: PhantomData,
                })
            }
        }
    };
}

impl_box_observer!(BoxAdminObserver, AdminObserver, WrappedAdminObserver);
impl_box_observer!(BoxQueryObserver, QueryObserver, WrappedQueryObserver);
impl_box_observer!(
    BoxApplyLightlikePersistenceObserver,
    ApplyLightlikePersistenceObserver,
    WrappedApplyLightlikePersistenceObserver
);
impl_box_observer_g!(
    BoxSplitCheckObserver,
    SplitCheckObserver,
    WrappedSplitCheckObserver
);
impl_box_observer!(BoxRoleObserver, RoleObserver, WrappedRoleObserver);
impl_box_observer!(
    BoxRegionChangeObserver,
    RegionChangeObserver,
    WrappedRegionChangeObserver
);
impl_box_observer!(BoxCmdObserver, CmdObserver, WrappedCmdObserver);

/// Registry contains all registered interlocks.
#[derive(Clone)]
pub struct Registry<E>
where
    E: 'static,
{
    admin_observers: Vec<Entry<BoxAdminObserver>>,
    query_observers: Vec<Entry<BoxQueryObserver>>,
    apply_lightlike_persistence_observers: Vec<Entry<BoxApplyLightlikePersistenceObserver>>,
    split_check_observers: Vec<Entry<BoxSplitCheckObserver<E>>>,
    role_observers: Vec<Entry<BoxRoleObserver>>,
    region_change_observers: Vec<Entry<BoxRegionChangeObserver>>,
    cmd_observers: Vec<Entry<BoxCmdObserver>>,
    // TODO: add endpoint
}

impl<E> Default for Registry<E> {
    fn default() -> Registry<E> {
        Registry {
            admin_observers: Default::default(),
            query_observers: Default::default(),
            apply_lightlike_persistence_observers: Default::default(),
            split_check_observers: Default::default(),
            role_observers: Default::default(),
            region_change_observers: Default::default(),
            cmd_observers: Default::default(),
        }
    }
}

macro_rules! push {
    ($p:expr, $t:solitonid, $vec:expr) => {
        $t.inner().start();
        let e = Entry {
            priority: $p,
            observer: $t,
        };
        let vec = &mut $vec;
        vec.push(e);
        vec.sort_by(|l, r| l.priority.cmp(&r.priority));
    };
}

impl<E> Registry<E> {
    pub fn register_admin_observer(&mut self, priority: u32, ao: BoxAdminObserver) {
        push!(priority, ao, self.admin_observers);
    }

    pub fn register_query_observer(&mut self, priority: u32, qo: BoxQueryObserver) {
        push!(priority, qo, self.query_observers);
    }

    pub fn register_apply_lightlike_persistence_observer(
        &mut self,
        priority: u32,
        aso: BoxApplyLightlikePersistenceObserver,
    ) {
        push!(priority, aso, self.apply_lightlike_persistence_observers);
    }

    pub fn register_split_check_observer(&mut self, priority: u32, sco: BoxSplitCheckObserver<E>) {
        push!(priority, sco, self.split_check_observers);
    }

    pub fn register_role_observer(&mut self, priority: u32, ro: BoxRoleObserver) {
        push!(priority, ro, self.role_observers);
    }

    pub fn register_region_change_observer(&mut self, priority: u32, rlo: BoxRegionChangeObserver) {
        push!(priority, rlo, self.region_change_observers);
    }

    pub fn register_cmd_observer(&mut self, priority: u32, rlo: BoxCmdObserver) {
        push!(priority, rlo, self.cmd_observers);
    }
}

/// A macro that loops over all observers and returns early when error is found or
/// bypass is set. `try_loop_ob` is expected to be used for hook that returns a `Result`.
macro_rules! try_loop_ob {
    ($r:expr, $obs:expr, $hook:solitonid, $($args:tt)*) => {
        loop_ob!(_imp _res, $r, $obs, $hook, $($args)*)
    };
}

/// A macro that loops over all observers and returns early when bypass is set.
///
/// Using a macro so we don't need to write tests for every observers.
macro_rules! loop_ob {
    // Execute a hook, return early if error is found.
    (_exec _res, $o:expr, $hook:solitonid, $ctx:expr, $($args:tt)*) => {
        $o.inner().$hook($ctx, $($args)*)?
    };
    // Execute a hook.
    (_exec _tup, $o:expr, $hook:solitonid, $ctx:expr, $($args:tt)*) => {
        $o.inner().$hook($ctx, $($args)*)
    };
    // When the try loop finishes successfully, the value to be returned.
    (_done _res) => {
        Ok(())
    };
    // When the loop finishes successfully, the value to be returned.
    (_done _tup) => {{}};
    // Actual implementation of the for loop.
    (_imp $res_type:tt, $r:expr, $obs:expr, $hook:solitonid, $($args:tt)*) => {{
        let mut ctx = ObserverContext::new($r);
        for o in $obs {
            loop_ob!(_exec $res_type, o.observer, $hook, &mut ctx, $($args)*);
            if ctx.bypass {
                break;
            }
        }
        loop_ob!(_done $res_type)
    }};
    // Loop over all observers and return early when bypass is set.
    // This macro is expected to be used for hook that returns `()`.
    ($r:expr, $obs:expr, $hook:solitonid, $($args:tt)*) => {
        loop_ob!(_imp _tup, $r, $obs, $hook, $($args)*)
    };
}

/// Admin and invoke all interlocks.
#[derive(Clone)]
pub struct InterlockHost<E>
where
    E: 'static,
{
    pub registry: Registry<E>,
}

impl<E> Default for InterlockHost<E>
where
    E: 'static,
{
    fn default() -> Self {
        InterlockHost {
            registry: Default::default(),
        }
    }
}

impl<E> InterlockHost<E>
where
    E: HiKV,
{
    pub fn new<C: CasualRouter<E::LightlikePersistence> + Clone + Send + 'static>(ch: C) -> InterlockHost<E> {
        let mut registry = Registry::default();
        registry.register_split_check_observer(
            200,
            BoxSplitCheckObserver::new(SizeCheckObserver::new(ch.clone())),
        );
        registry.register_split_check_observer(
            200,
            BoxSplitCheckObserver::new(KeysCheckObserver::new(ch)),
        );
        // TableCheckObserver has higher priority than SizeCheckObserver.
        registry.register_split_check_observer(100, BoxSplitCheckObserver::new(HalfCheckObserver));
        registry.register_split_check_observer(
            400,
            BoxSplitCheckObserver::new(TableCheckObserver::default()),
        );
        InterlockHost { registry }
    }

    /// Call all propose hooks until bypass is set to true.
    pub fn pre_propose(&self, region: &Region, req: &mut violetabftCmdRequest) -> Result<()> {
        if !req.has_admin_request() {
            let query = req.mut_requests();
            let mut vec_query = mem::take(query).into();
            let result = try_loop_ob!(
                region,
                &self.registry.query_observers,
                pre_propose_query,
                &mut vec_query,
            );
            *query = vec_query.into();
            result
        } else {
            let admin = req.mut_admin_request();
            try_loop_ob!(
                region,
                &self.registry.admin_observers,
                pre_propose_admin,
                admin
            )
        }
    }

    /// Call all pre apply hook until bypass is set to true.
    pub fn pre_apply(&self, region: &Region, req: &violetabftCmdRequest) {
        if !req.has_admin_request() {
            let query = req.get_requests();
            loop_ob!(
                region,
                &self.registry.query_observers,
                pre_apply_query,
                query,
            );
        } else {
            let admin = req.get_admin_request();
            loop_ob!(
                region,
                &self.registry.admin_observers,
                pre_apply_admin,
                admin
            );
        }
    }

    pub fn post_apply(&self, region: &Region, resp: &mut violetabftCmdResponse) {
        if !resp.has_admin_response() {
            let query = resp.mut_responses();
            let mut vec_query = mem::take(query).into();
            loop_ob!(
                region,
                &self.registry.query_observers,
                post_apply_query,
                &mut vec_query,
            );
            *query = vec_query.into();
        } else {
            let admin = resp.mut_admin_response();
            loop_ob!(
                region,
                &self.registry.admin_observers,
                post_apply_admin,
                admin
            );
        }
    }

    pub fn pre_apply_plain_ehikvs_from_lightlike_persistence(
        &self,
        region: &Region,
        brane: BRANEName,
        ehikv_pairs: &[(Vec<u8>, Vec<u8>)],
    ) {
        loop_ob!(
            region,
            &self.registry.apply_lightlike_persistence_observers,
            pre_apply_plain_ehikvs,
            brane,
            ehikv_pairs
        );
    }

    pub fn pre_apply_Causet_from_lightlike_persistence(&self, region: &Region, brane: BRANEName, local_path: &str) {
        loop_ob!(
            region,
            &self.registry.apply_lightlike_persistence_observers,
            pre_apply_Causet,
            brane,
            local_path
        );
    }

    pub fn new_split_checker_host<'a>(
        &self,
        braneg: &'a Config,
        region: &Region,
        einstein_merkle_tree: &E,
        auto_split: bool,
        policy: CheckPolicy,
    ) -> SplitCheckerHost<'a, E> {
        let mut host = SplitCheckerHost::new(auto_split, braneg);
        loop_ob!(
            region,
            &self.registry.split_check_observers,
            add_checker,
            &mut host,
            einstein_merkle_tree,
            policy
        );
        host
    }

    pub fn on_role_change(&self, region: &Region, role: StateRole) {
        loop_ob!(region, &self.registry.role_observers, on_role_change, role);
    }

    pub fn on_region_changed(&self, region: &Region, event: RegionChangeEvent, role: StateRole) {
        loop_ob!(
            region,
            &self.registry.region_change_observers,
            on_region_changed,
            event,
            role
        );
    }

    pub fn prepare_for_apply(&self, observe_id: ObserveID, region_id: u64) {
        for cmd_ob in &self.registry.cmd_observers {
            cmd_ob
                .observer
                .inner()
                .on_prepare_for_apply(observe_id, region_id);
        }
    }

    pub fn on_apply_cmd(&self, observe_id: ObserveID, region_id: u64, cmd: Cmd) {
        assert!(
            !self.registry.cmd_observers.is_empty(),
            "CmdObserver is not registered"
        );
        for i in 0..self.registry.cmd_observers.len() - 1 {
            self.registry
                .cmd_observers
                .get(i)
                .unwrap()
                .observer
                .inner()
                .on_apply_cmd(observe_id, region_id, cmd.clone())
        }
        self.registry
            .cmd_observers
            .last()
            .unwrap()
            .observer
            .inner()
            .on_apply_cmd(observe_id, region_id, cmd)
    }

    pub fn on_flush_apply(&self, txn_extras: Vec<TxnExtra>) {
        if self.registry.cmd_observers.is_empty() {
            return;
        }
        for i in 0..self.registry.cmd_observers.len() - 1 {
            self.registry
                .cmd_observers
                .get(i)
                .unwrap()
                .observer
                .inner()
                .on_flush_apply(txn_extras.clone())
        }
        self.registry
            .cmd_observers
            .last()
            .unwrap()
            .observer
            .inner()
            .on_flush_apply(txn_extras)
    }

    pub fn shutdown(&self) {
        for entry in &self.registry.admin_observers {
            entry.observer.inner().stop();
        }
        for entry in &self.registry.query_observers {
            entry.observer.inner().stop();
        }
        for entry in &self.registry.split_check_observers {
            entry.observer.inner().stop();
        }
        for entry in &self.registry.cmd_observers {
            entry.observer.inner().stop();
        }
    }
}

#[braneg(test)]
mod tests {
    use crate::interlock::*;
    use std::sync::atomic::*;
    use std::sync::Arc;

    use einstein_merkle_tree_foundationeinsteindb::foundationeinsteindbeinstein_merkle_tree;
    use ehikvproto::metapb::Region;
    use ehikvproto::violetabft_cmdpb::{
        AdminRequest, AdminResponse, violetabftCmdRequest, violetabftCmdResponse, Request, Response,
    };

    #[derive(Clone, Default)]
    struct Testinterlock {
        bypass: Arc<AtomicBool>,
        called: Arc<AtomicUsize>,
        return_err: Arc<AtomicBool>,
    }

    impl interlock for Testinterlock {}

    impl AdminObserver for Testinterlock {
        fn pre_propose_admin(
            &self,
            ctx: &mut ObserverContext<'_>,
            _: &mut AdminRequest,
        ) -> Result<()> {
            self.called.fetch_add(1, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
            if self.return_err.load(Ordering::SeqCst) {
                return Err(box_err!("error"));
            }
            Ok(())
        }

        fn pre_apply_admin(&self, ctx: &mut ObserverContext<'_>, _: &AdminRequest) {
            self.called.fetch_add(2, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
        }

        fn post_apply_admin(&self, ctx: &mut ObserverContext<'_>, _: &mut AdminResponse) {
            self.called.fetch_add(3, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
        }
    }

    impl QueryObserver for Testinterlock {
        fn pre_propose_query(
            &self,
            ctx: &mut ObserverContext<'_>,
            _: &mut Vec<Request>,
        ) -> Result<()> {
            self.called.fetch_add(4, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
            if self.return_err.load(Ordering::SeqCst) {
                return Err(box_err!("error"));
            }
            Ok(())
        }

        fn pre_apply_query(&self, ctx: &mut ObserverContext<'_>, _: &[Request]) {
            self.called.fetch_add(5, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
        }

        fn post_apply_query(&self, ctx: &mut ObserverContext<'_>, _: &mut Vec<Response>) {
            self.called.fetch_add(6, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
        }
    }

    impl RoleObserver for Testinterlock {
        fn on_role_change(&self, ctx: &mut ObserverContext<'_>, _: StateRole) {
            self.called.fetch_add(7, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
        }
    }

    impl RegionChangeObserver for Testinterlock {
        fn on_region_changed(
            &self,
            ctx: &mut ObserverContext<'_>,
            _: RegionChangeEvent,
            _: StateRole,
        ) {
            self.called.fetch_add(8, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
        }
    }

    impl ApplyLightlikePersistenceObserver for Testinterlock {
        fn pre_apply_plain_ehikvs(
            &self,
            ctx: &mut ObserverContext<'_>,
            _: BRANEName,
            _: &[(Vec<u8>, Vec<u8>)],
        ) {
            self.called.fetch_add(9, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
        }

        fn pre_apply_Causet(&self, ctx: &mut ObserverContext<'_>, _: BRANEName, _: &str) {
            self.called.fetch_add(10, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
        }
    }

    impl CmdObserver for Testinterlock {
        fn on_prepare_for_apply(&self, _: ObserveID, _: u64) {
            self.called.fetch_add(11, Ordering::SeqCst);
        }
        fn on_apply_cmd(&self, _: ObserveID, _: u64, _: Cmd) {
            self.called.fetch_add(12, Ordering::SeqCst);
        }
        fn on_flush_apply(&self, _: Vec<TxnExtra>) {
            self.called.fetch_add(13, Ordering::SeqCst);
        }
    }

    macro_rules! assert_all {
        ($target:expr, $expect:expr) => {{
            for (c, e) in ($target).iter().zip($expect) {
                assert_eq!(c.load(Ordering::SeqCst), *e);
            }
        }};
    }

    macro_rules! set_all {
        ($target:expr, $v:expr) => {{
            for v in $target {
                v.store($v, Ordering::SeqCst);
            }
        }};
    }

    #[test]
    fn test_trigger_right_hook() {
        let mut host = interlockHost::<foundationeinsteindbeinstein_merkle_tree>::default();
        let ob = Testinterlock::default();
        host.registry
            .register_admin_observer(1, BoxAdminObserver::new(ob.clone()));
        host.registry
            .register_query_observer(1, BoxQueryObserver::new(ob.clone()));
        host.registry
            .register_apply_lightlike_persistence_observer(1, BoxApplyLightlikePersistenceObserver::new(ob.clone()));
        host.registry
            .register_role_observer(1, BoxRoleObserver::new(ob.clone()));
        host.registry
            .register_region_change_observer(1, BoxRegionChangeObserver::new(ob.clone()));
        host.registry
            .register_cmd_observer(1, BoxCmdObserver::new(ob.clone()));
        let region = Region::default();
        let mut admin_req = violetabftCmdRequest::default();
        admin_req.set_admin_request(AdminRequest::default());
        host.pre_propose(&region, &mut admin_req).unwrap();
        assert_all!(&[&ob.called], &[1]);
        host.pre_apply(&region, &admin_req);
        assert_all!(&[&ob.called], &[3]);
        let mut admin_resp = violetabftCmdResponse::default();
        admin_resp.set_admin_response(AdminResponse::default());
        host.post_apply(&region, &mut admin_resp);
        assert_all!(&[&ob.called], &[6]);

        let mut query_req = violetabftCmdRequest::default();
        query_req.set_requests(vec![Request::default()].into());
        host.pre_propose(&region, &mut query_req).unwrap();
        assert_all!(&[&ob.called], &[10]);
        host.pre_apply(&region, &query_req);
        assert_all!(&[&ob.called], &[15]);
        let mut query_resp = admin_resp;
        query_resp.clear_admin_response();
        host.post_apply(&region, &mut query_resp);
        assert_all!(&[&ob.called], &[21]);

        host.on_role_change(&region, StateRole::Leader);
        assert_all!(&[&ob.called], &[28]);

        host.on_region_changed(&region, RegionChangeEvent::Create, StateRole::Follower);
        assert_all!(&[&ob.called], &[36]);

        host.pre_apply_plain_ehikvs_from_lightlike_persistence(&region, "default", &[]);
        assert_all!(&[&ob.called], &[45]);
        host.pre_apply_Causet_from_lightlike_persistence(&region, "default", "");
        assert_all!(&[&ob.called], &[55]);
        let observe_id = ObserveID::new();
        host.prepare_for_apply(observe_id, 0);
        assert_all!(&[&ob.called], &[66]);
        host.on_apply_cmd(
            observe_id,
            0,
            Cmd::new(0, violetabftCmdRequest::default(), query_resp),
        );
        assert_all!(&[&ob.called], &[78]);
        host.on_flush_apply(Vec::default());
        assert_all!(&[&ob.called], &[91]);
    }

    #[test]
    fn test_order() {
        let mut host = interlockHost::<foundationeinsteindbeinstein_merkle_tree>::default();

        let ob1 = Testinterlock::default();
        host.registry
            .register_admin_observer(3, BoxAdminObserver::new(ob1.clone()));
        host.registry
            .register_query_observer(3, BoxQueryObserver::new(ob1.clone()));
        let ob2 = Testinterlock::default();
        host.registry
            .register_admin_observer(2, BoxAdminObserver::new(ob2.clone()));
        host.registry
            .register_query_observer(2, BoxQueryObserver::new(ob2.clone()));

        let region = Region::default();
        let mut admin_req = violetabftCmdRequest::default();
        admin_req.set_admin_request(AdminRequest::default());
        let mut admin_resp = violetabftCmdResponse::default();
        admin_resp.set_admin_response(AdminResponse::default());
        let query_req = violetabftCmdRequest::default();
        let query_resp = violetabftCmdResponse::default();

        let cases = vec![(0, admin_req, admin_resp), (3, query_req, query_resp)];

        for (base_score, mut req, mut resp) in cases {
            set_all!(&[&ob1.return_err, &ob2.return_err], false);
            set_all!(&[&ob1.called, &ob2.called], 0);
            set_all!(&[&ob1.bypass, &ob2.bypass], true);

            host.pre_propose(&region, &mut req).unwrap();

            // less means more.
            assert_all!(&[&ob1.called, &ob2.called], &[0, base_score + 1]);

            host.pre_apply(&region, &req);
            assert_all!(&[&ob1.called, &ob2.called], &[0, base_score * 2 + 3]);

            host.post_apply(&region, &mut resp);
            assert_all!(&[&ob1.called, &ob2.called], &[0, base_score * 3 + 6]);

            set_all!(&[&ob2.bypass], false);
            set_all!(&[&ob2.called], 0);

            host.pre_propose(&region, &mut req).unwrap();

            assert_all!(
                &[&ob1.called, &ob2.called],
                &[base_score + 1, base_score + 1]
            );

            set_all!(&[&ob1.called, &ob2.called], 0);

            // when return error, following interlocking_dir should not be run.
            set_all!(&[&ob2.return_err], true);
            host.pre_propose(&region, &mut req).unwrap_err();
            assert_all!(&[&ob1.called, &ob2.called], &[0, base_score + 1]);
        }
    }
}
