package scala.scalanative
package codegen

import scala.collection.mutable

import scala.scalanative.linker.ReachabilityAnalysis
import scala.scalanative.nir.ControlFlow

private[codegen] object BoundaryOpt {
  private val BoundaryModule = nir.Global.Top("scala.util.boundary$")
  private val BoundaryLabel = nir.Global.Top("scala.util.boundary$Label")
  private val BoundaryBreak = nir.Global.Top("scala.util.boundary$Break")

  private val BoundaryLabelRef = nir.Type.Ref(BoundaryLabel)
  private val BoundaryBreakRef = nir.Type.Ref(BoundaryBreak)

  final case class Prepared(
      activeBoundariesByMethod: Map[
        nir.Global.Member,
        Map[nir.Local, Candidate]
      ],
      safeBreakSitesByMethod: Map[nir.Global.Member, Set[nir.Local]]
  ) {
    def activeBoundaries(
        method: nir.Global.Member
    ): Map[nir.Local, Candidate] =
      activeBoundariesByMethod.getOrElse(method, Map.empty)

    def safeBreakSites(method: nir.Global.Member): Set[nir.Local] =
      safeBreakSitesByMethod.getOrElse(method, Set.empty)
  }

  final case class Candidate(
      method: nir.Global.Member,
      setupBlock: nir.Local,
      bodyEntry: nir.Local,
      handlerLabel: nir.Local,
      resultLabel: nir.Local,
      resultTy: nir.Type,
      localLabel: nir.Local,
      catchSuccessPred: nir.Local,
      normalSuccessPreds: Set[nir.Local]
  )

  private sealed trait Origin
  private final case class Param(index: Int) extends Origin
  private final case class LocalBoundary(local: nir.Local) extends Origin

  private final case class MethodInfo(
      defn: nir.Defn.Define,
      cfg: ControlFlow.Graph,
      methodLocals: Map[nir.Local, nir.Global.Member],
      candidates: Seq[Candidate],
      transparent: Boolean
  ) {
    lazy val params: Seq[nir.Val.Local] =
      defn.insts.head.asInstanceOf[nir.Inst.Label].params
  }

  def prepare(
      defns: Seq[nir.Defn],
      supported: Boolean
  )(implicit analysis: ReachabilityAnalysis.Result): Prepared = {
    if (!supported) Prepared(Map.empty, Map.empty)
    else {
      val methods = defns.collect {
        case defn: nir.Defn.Define => defn.name -> methodInfo(defn)
      }.toMap

      val activeBoundaries =
        mutable.Map.empty[nir.Global.Member, mutable.Map[nir.Local, Candidate]]
      val safeBreakSites =
        mutable.Map.empty[nir.Global.Member, mutable.Set[nir.Local]]

      def addBreakSite(method: nir.Global.Member, site: nir.Local): Unit = {
        val sites = safeBreakSites.getOrElseUpdate(method, mutable.Set.empty)
        sites += site
      }

      def addActiveBoundary(candidate: Candidate): Unit = {
        val boundaries =
          activeBoundaries.getOrElseUpdate(candidate.method, mutable.Map.empty)
        boundaries(candidate.handlerLabel) = candidate
      }

      methods.valuesIterator.foreach { info =>
        info.candidates.foreach { candidate =>
          val visited = mutable.Set.empty[(nir.Global.Member, Origin)]
          var foundPair = false

          def currentOriginLocal(
              info: MethodInfo,
              origin: Origin
          ): Option[nir.Local] = origin match {
            case Param(index) if index >= 0 && index < info.params.length =>
              Some(info.params(index).id)
            case LocalBoundary(local) if info.defn.insts.exists {
                  case nir.Inst.Let(`local`, _, _) => true
                  case _                           => false
                } =>
              Some(local)
            case _ =>
              None
          }

          def loop(method: nir.Global.Member, origin: Origin): Unit = {
            if (!visited.add((method, origin))) return

            methods.get(method).foreach { info =>
              if (info.transparent) {
                currentOriginLocal(info, origin).foreach { originLocal =>
                  info.defn.insts.foreach {
                    case nir.Inst.Let(
                          site,
                          nir.Op.Call(_, target, args),
                          _
                        ) =>
                      callTarget(target, info.methodLocals).foreach {
                        case callee if isBoundaryBreak(callee) =>
                          breakLabel(args).foreach {
                            case nir.Val.Local(`originLocal`, _) =>
                              addBreakSite(method, site)
                              foundPair = true
                            case _ => ()
                          }

                        case callee
                            if methods.get(callee).exists(_.transparent) =>
                          args.zipWithIndex.foreach {
                            case (nir.Val.Local(`originLocal`, _), index)
                                if isBoundaryLabelArg(args.lift(index)) =>
                              loop(callee, Param(index))
                            case _ => ()
                          }

                        case _ => ()
                      }
                    case _ => ()
                  }
                }
              }
            }
          }

          loop(candidate.method, LocalBoundary(candidate.localLabel))
          if (foundPair) addActiveBoundary(candidate)
        }
      }

      Prepared(
        activeBoundariesByMethod = activeBoundaries.iterator.map {
          case (method, boundaries) => method -> boundaries.toMap
        }.toMap,
        safeBreakSitesByMethod = safeBreakSites.iterator.map {
          case (method, sites) => method -> sites.toSet
        }.toMap
      )
    }
  }

  def rewrite(
      defn: nir.Defn.Define,
      prepared: Prepared,
      frameTy: nir.Type.StructValue
  ): nir.Defn.Define = {
    val method = defn.name
    val activeBoundaries = prepared.activeBoundaries(method)
    val safeBreakSites = prepared.safeBreakSites(method)
    if (activeBoundaries.isEmpty && safeBreakSites.isEmpty) defn
    else {
      val cfg = ControlFlow.Graph(defn.insts)
      val fresh = nir.Fresh(defn.insts)
      val methodLocals = resolveMethodLocals(defn)

      val boundaryState = activeBoundaries.valuesIterator.map { candidate =>
        val frame = fresh()
        val normalExit = fresh()
        val fastExit = fresh()
        candidate.handlerLabel -> RuntimeState(
          candidate,
          frame,
          normalExit,
          fastExit
        )
      }.toMap

      val out = mutable.UnrolledBuffer.empty[nir.Inst]

      cfg.all.foreach { block =>
        implicit val blockPos: nir.SourcePosition = block.pos
        val stateOpt =
          boundaryState.values.find(_.candidate.setupBlock == block.id)
        val handlerStateOpt = boundaryState.get(block.id)
        val normalExitStateOpt =
          boundaryState.values.find(
            _.candidate.normalSuccessPreds.contains(block.id)
          )

        out += block.label

        val body = block.insts.init
        var appendedCf: Option[nir.Inst.Cf] = None

        body.foreach {
          case inst @ nir.Inst.Let(site, nir.Op.Call(_, target, args), _)
              if safeBreakSites.contains(site) &&
                callTarget(target, methodLocals).exists(isBoundaryBreak) =>
            out += nir.Inst.Let(
              fresh(),
              nir.Op.Call(
                Lower.BoundaryBreakFastSig,
                Lower.BoundaryBreakFast,
                Seq(args.last, args(args.length - 2))
              ),
              nir.Next.None
            )(inst.pos, inst.scopeId)
            out += inst

          case other =>
            out += other
        }

        val cf = block.insts.last.asInstanceOf[nir.Inst.Cf]
        appendedCf = Some {
          stateOpt match {
            case Some(state) =>
              rewriteSetupBlock(state, block, cf, out, fresh, frameTy)
            case None =>
              handlerStateOpt match {
                case Some(state) =>
                  prependBoundaryPop(state, out, cf, fresh)
                case None =>
                  normalExitStateOpt match {
                    case Some(state) =>
                      rewriteNormalSuccessBlock(state, cf)
                    case None =>
                      cf
                  }
              }
          }
        }

        out += appendedCf.get
      }

      boundaryState.values.foreach { state =>
        emitNormalExitBlock(state, out, fresh)
        emitFastExitBlock(state, out, fresh)
      }

      implicit val pos: nir.SourcePosition = defn.pos
      defn.copy(insts = out.toSeq)
    }
  }

  private final case class RuntimeState(
      candidate: Candidate,
      frameLocal: nir.Local,
      normalExitLabel: nir.Local,
      fastExitLabel: nir.Local
  )

  private def methodInfo(
      defn: nir.Defn.Define
  )(implicit analysis: ReachabilityAnalysis.Result): MethodInfo = {
    val cfg = ControlFlow.Graph(defn.insts)
    val methodLocals = resolveMethodLocals(defn)
    val candidates = findCandidates(defn, cfg)
    val handlerLabels = candidates.iterator.map(_.handlerLabel).toSet

    val transparent = defn.insts.forall {
      case nir.Inst.Let(_, _, nir.Next.None) => true
      case nir.Inst.Let(_, _, unwind)        =>
        unwindTarget(unwind).exists(handlerLabels.contains)
      case nir.Inst.Throw(_, nir.Next.None)    => true
      case nir.Inst.Unreachable(nir.Next.None) => true
      case nir.Inst.Throw(_, _)                => false
      case nir.Inst.Unreachable(_)             => false
      case _                                   => true
    }

    MethodInfo(defn, cfg, methodLocals, candidates, transparent)
  }

  private def resolveMethodLocals(
      defn: nir.Defn.Define
  ): Map[nir.Local, nir.Global.Member] = {
    val resolved = mutable.Map.empty[nir.Local, nir.Global.Member]
    defn.insts.foreach {
      case nir.Inst.Let(id, nir.Op.Method(value, sig), _) =>
        value.ty match {
          case owner: nir.Type.RefKind =>
            resolved(id) = owner.className.member(sig)
          case _ => ()
        }
      case _ => ()
    }
    resolved.toMap
  }

  private def findCandidates(
      defn: nir.Defn.Define,
      cfg: ControlFlow.Graph
  ): Seq[Candidate] = {
    cfg.all.flatMap { handler =>
      matchHandlerBlock(handler, cfg).flatMap {
        case HandlerPattern(
              localLabel,
              catchSuccessPred,
              resultLabel,
              resultTy
            ) =>
          findSetupBlock(cfg, localLabel).flatMap { setup =>
            val nir.Inst.Jump(nir.Next.Label(bodyEntry, _)) =
              setup.insts.last: @unchecked
            val (
              normalizedCatchSuccessPred,
              normalizedResultLabel
            ) =
              normalizeResultTarget(cfg, catchSuccessPred, resultLabel)
            val resultBlock = cfg.find(normalizedResultLabel)
            val normalPreds = resultBlock.pred
              .map(_.id)
              .filterNot(_ == normalizedCatchSuccessPred)
              .toSet
            if (normalPreds.isEmpty) None
            else
              Some(
                Candidate(
                  method = defn.name,
                  setupBlock = setup.id,
                  bodyEntry = bodyEntry,
                  handlerLabel = handler.id,
                  resultLabel = normalizedResultLabel,
                  resultTy = resultTy,
                  localLabel = localLabel,
                  catchSuccessPred = normalizedCatchSuccessPred,
                  normalSuccessPreds = normalPreds
                )
              )
          }
      }
    }
  }

  private final case class HandlerPattern(
      localLabel: nir.Local,
      catchSuccessPred: nir.Local,
      resultLabel: nir.Local,
      resultTy: nir.Type
  )

  private def matchHandlerBlock(
      handler: ControlFlow.Block,
      cfg: ControlFlow.Graph
  ): Option[HandlerPattern] = {
    val ex = handler.params.headOption
    (ex, handler.insts.toList) match {
      case (
            Some(exv),
            List(
              nir.Inst.Let(
                isId,
                nir.Op.Is(BoundaryBreakRef, checkedValue),
                nir.Next.None
              ),
              nir.Inst.If(
                nir.Val.Local(condLocal, nir.Type.Bool),
                nir.Next.Label(castLabel, Seq()),
                nir.Next.Label(nonBreakLabel, Seq())
              )
            )
          ) if checkedValue == exv && condLocal == isId =>
        val nonBreakBlock = cfg.find(nonBreakLabel)
        val castBlock = cfg.find(castLabel)
        if (!isRethrowBlock(nonBreakBlock, exv.id)) None
        else
          matchCastBlock(castBlock, exv.id, cfg)
      case _ =>
        None
    }
  }

  private def matchCastBlock(
      block: ControlFlow.Block,
      throwableLocal: nir.Local,
      cfg: ControlFlow.Graph
  ): Option[HandlerPattern] = {
    block.insts.toList match {
      case List(
            nir.Inst.Let(
              exBreak,
              nir.Op.As(BoundaryBreakRef, castValue),
              nir.Next.None
            ),
            nir.Inst.Let(
              methodId,
              nir.Op.Method(methodValue, sig),
              nir.Next.None
            ),
            nir.Inst.Let(
              sameId,
              nir.Op.Call(
                _,
                calledMethod,
                Seq(exArg, nir.Val.Local(localLabel, _))
              ),
              nir.Next.None
            ),
            nir.Inst.If(
              nir.Val.Local(condLocal, nir.Type.Bool),
              nir.Next.Label(matchLabel, Seq()),
              nir.Next.Label(mismatchLabel, Seq())
            )
          )
          if castValue == nir.Val.Local(throwableLocal, nir.Rt.Throwable) &&
            methodValue == nir.Val.Local(exBreak, BoundaryBreakRef) &&
            calledMethod == nir.Val.Local(methodId, nir.Type.Ptr) &&
            exArg == nir.Val.Local(exBreak, BoundaryBreakRef) &&
            condLocal == sameId &&
            isIsSameLabelAs(sig) =>
        val mismatchBlock = cfg.find(mismatchLabel)
        val matchBlock = cfg.find(matchLabel)
        if (!isRethrowBlock(mismatchBlock, exBreak)) None
        else
          matchBlock.insts.last match {
            case nir.Inst.Jump(nir.Next.Label(resultLabel, Seq(resultVal))) =>
              Some(
                HandlerPattern(
                  localLabel = localLabel,
                  catchSuccessPred = matchBlock.id,
                  resultLabel = resultLabel,
                  resultTy = resultVal.ty
                )
              )
            case _ =>
              None
          }
      case _ =>
        None
    }
  }

  private def findSetupBlock(
      cfg: ControlFlow.Graph,
      localLabel: nir.Local
  ): Option[ControlFlow.Block] = {
    cfg.all.find { block =>
      val hasJump = block.insts.last match {
        case nir.Inst.Jump(nir.Next.Label(_, _)) => true
        case _                                   => false
      }
      val hasLabelAlloc = block.insts.exists {
        case nir.Inst.Let(
              `localLabel`,
              nir.Op.Classalloc(`BoundaryLabel`, _),
              _
            ) =>
          true
        case _ => false
      }
      hasJump && hasLabelAlloc
    }
  }

  private def normalizeResultTarget(
      cfg: ControlFlow.Graph,
      catchSuccessPred: nir.Local,
      resultLabel: nir.Local
  ): (nir.Local, nir.Local) = {
    val resultBlock = cfg.find(resultLabel)
    resultBlock.params.toList match {
      case param :: Nil =>
        resultBlock.insts.toList match {
          case List(
                nir.Inst.Jump(
                  nir.Next.Label(
                    finalResultLabel,
                    Seq(nir.Val.Local(argLocal, _))
                  )
                )
              ) if argLocal == param.id =>
            resultBlock.id -> finalResultLabel
          case _ =>
            catchSuccessPred -> resultLabel
        }
      case _ =>
        catchSuccessPred -> resultLabel
    }
  }

  private def isRethrowBlock(
      block: ControlFlow.Block,
      local: nir.Local
  ): Boolean =
    block.insts.toList match {
      case List(nir.Inst.Throw(nir.Val.Local(`local`, _), nir.Next.None)) =>
        true
      case _ => false
    }

  private def isIsSameLabelAs(sig: nir.Sig): Boolean =
    sig.unmangled match {
      case nir.Sig.Method("isSameLabelAs", _, _) => true
      case _                                     => false
    }

  private def unwindTarget(next: nir.Next): Option[nir.Local] = next match {
    case nir.Next.Unwind(_, nir.Next.Label(id, _)) => Some(id)
    case _                                         => None
  }

  private def callTarget(
      value: nir.Val,
      methodLocals: Map[nir.Local, nir.Global.Member]
  ): Option[nir.Global.Member] = value match {
    case nir.Val.Global(member: nir.Global.Member, _) => Some(member)
    case nir.Val.Local(id, _)                         => methodLocals.get(id)
    case _                                            => None
  }

  private def isBoundaryBreak(symbol: nir.Global.Member): Boolean =
    symbol.owner == BoundaryModule &&
      (symbol.sig.unmangled match {
        case nir.Sig.Method("break", _, _) =>
          true
        case _ => false
      })

  private def breakLabel(args: Seq[nir.Val]): Option[nir.Val] =
    if (args.length >= 2) args.lastOption else None

  private def isBoundaryLabelArg(arg: Option[nir.Val]): Boolean =
    arg.exists {
      case value =>
        value.ty match {
          case ty: nir.Type.RefKind => ty.className == BoundaryLabel
          case _                    => false
        }
    }

  private def rewriteSetupBlock(
      state: RuntimeState,
      block: ControlFlow.Block,
      cf: nir.Inst.Cf,
      out: mutable.UnrolledBuffer[nir.Inst],
      fresh: nir.Fresh,
      frameTy: nir.Type.StructValue
  ): nir.Inst.Cf = {
    implicit val pos: nir.SourcePosition = block.pos
    implicit val scopeId: nir.ScopeId = nir.ScopeId.TopLevel

    out += nir.Inst.Let(
      state.frameLocal,
      nir.Op.Stackalloc(frameTy, nir.Val.Int(1)),
      nir.Next.None
    )(pos, scopeId)
    out += nir.Inst.Let(
      fresh(),
      nir.Op.Call(
        Lower.BoundaryPushSig,
        Lower.BoundaryPush,
        Seq(
          nir.Val.Local(state.frameLocal, nir.Type.Ptr),
          nir.Val.Local(state.candidate.localLabel, BoundaryLabelRef)
        )
      ),
      nir.Next.None
    )(pos, scopeId)
    val setjmpResult = fresh()
    out += nir.Inst.Let(
      setjmpResult,
      nir.Op.Call(
        Lower.BoundarySetjmpSig,
        Lower.BoundarySetjmp,
        Seq(nir.Val.Local(state.frameLocal, nir.Type.Ptr))
      ),
      nir.Next.None
    )(pos, scopeId)

    cf match {
      case nir.Inst.Jump(nir.Next.Label(bodyEntry, args)) =>
        nir.Inst.If(
          nir.Val.Local(setjmpResult, nir.Type.Bool),
          nir.Next.Label(state.fastExitLabel, Seq()),
          nir.Next.Label(bodyEntry, args)
        )
      case _ =>
        cf
    }
  }

  private def prependBoundaryPop(
      state: RuntimeState,
      out: mutable.UnrolledBuffer[nir.Inst],
      cf: nir.Inst.Cf,
      fresh: nir.Fresh
  ): nir.Inst.Cf = {
    implicit val pos: nir.SourcePosition = cf.pos
    implicit val scopeId: nir.ScopeId = nir.ScopeId.TopLevel
    out += nir.Inst.Let(
      fresh(),
      nir.Op.Call(
        Lower.BoundaryPopSig,
        Lower.BoundaryPop,
        Seq(nir.Val.Local(state.frameLocal, nir.Type.Ptr))
      ),
      nir.Next.None
    )(pos, scopeId)
    cf
  }

  private def rewriteNormalSuccessBlock(
      state: RuntimeState,
      cf: nir.Inst.Cf
  ): nir.Inst.Cf = cf match {
    case nir.Inst.Jump(nir.Next.Label(label, args))
        if label == state.candidate.resultLabel =>
      nir.Inst.Jump(nir.Next.Label(state.normalExitLabel, args))(cf.pos)
    case _ =>
      cf
  }

  private def emitNormalExitBlock(
      state: RuntimeState,
      out: mutable.UnrolledBuffer[nir.Inst],
      fresh: nir.Fresh
  ): Unit = {
    implicit val pos: nir.SourcePosition = nir.SourcePosition.NoPosition
    implicit val scopeId: nir.ScopeId = nir.ScopeId.TopLevel
    val param = nir.Val.Local(fresh(), state.candidate.resultTy)
    out += nir.Inst.Label(state.normalExitLabel, Seq(param))
    out += nir.Inst.Let(
      fresh(),
      nir.Op.Call(
        Lower.BoundaryPopSig,
        Lower.BoundaryPop,
        Seq(nir.Val.Local(state.frameLocal, nir.Type.Ptr))
      ),
      nir.Next.None
    )(pos, scopeId)
    out += nir.Inst.Jump(
      nir.Next.Label(state.candidate.resultLabel, Seq(param))
    )
  }

  private def emitFastExitBlock(
      state: RuntimeState,
      out: mutable.UnrolledBuffer[nir.Inst],
      fresh: nir.Fresh
  ): Unit = {
    implicit val pos: nir.SourcePosition = nir.SourcePosition.NoPosition
    implicit val scopeId: nir.ScopeId = nir.ScopeId.TopLevel

    out += nir.Inst.Label(state.fastExitLabel, Seq.empty)
    val resultObj = fresh()
    out += nir.Inst.Let(
      resultObj,
      nir.Op.Call(
        Lower.BoundaryResultSig,
        Lower.BoundaryResult,
        Seq(nir.Val.Local(state.frameLocal, nir.Type.Ptr))
      ),
      nir.Next.None
    )(pos, scopeId)
    val converted =
      convertResult(
        nir.Val.Local(resultObj, nir.Rt.Object),
        state.candidate.resultTy,
        fresh,
        out
      )
    out += nir.Inst.Let(
      fresh(),
      nir.Op.Call(
        Lower.BoundaryPopSig,
        Lower.BoundaryPop,
        Seq(nir.Val.Local(state.frameLocal, nir.Type.Ptr))
      ),
      nir.Next.None
    )(pos, scopeId)
    out += nir.Inst.Jump(
      nir.Next.Label(state.candidate.resultLabel, Seq(converted))
    )
  }

  private def castToPtr(
      value: nir.Val,
      fresh: nir.Fresh,
      pos: nir.SourcePosition,
      scopeId: nir.ScopeId,
      out: mutable.UnrolledBuffer[nir.Inst]
  ): nir.Val = {
    if (value.ty == nir.Type.Ptr) value
    else {
      val local = fresh()
      out += nir.Inst.Let(
        local,
        nir.Op.Conv(nir.Conv.Bitcast, nir.Type.Ptr, value),
        nir.Next.None
      )(pos, scopeId)
      nir.Val.Local(local, nir.Type.Ptr)
    }
  }

  private def convertResult(
      resultObj: nir.Val,
      targetTy: nir.Type,
      fresh: nir.Fresh,
      out: mutable.UnrolledBuffer[nir.Inst]
  ): nir.Val = {
    implicit val pos: nir.SourcePosition = nir.SourcePosition.NoPosition
    implicit val scopeId: nir.ScopeId = nir.ScopeId.TopLevel

    targetTy match {
      case nir.Type.Unit =>
        nir.Val.Unit
      case ty: nir.Type.RefKind =>
        val local = fresh()
        out += nir.Inst.Let(
          local,
          nir.Op.Conv(nir.Conv.Bitcast, ty, resultObj),
          nir.Next.None
        )
        nir.Val.Local(local, ty)
      case ty if nir.Type.box.contains(ty) =>
        val local = fresh()
        out += nir.Inst.Let(
          local,
          nir.Op.Unbox(nir.Type.box(ty), resultObj),
          nir.Next.None
        )
        nir.Val.Local(local, ty)
      case _ =>
        resultObj
    }
  }
}
