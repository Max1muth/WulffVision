import {
  Component, ElementRef, OnDestroy, ViewChild, AfterViewInit,
  ChangeDetectionStrategy, Input, OnChanges, Output, EventEmitter, SimpleChanges
} from '@angular/core';
import * as THREE from 'three';
import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls.js';

@Component({
  selector: 'app-wulff-net',
  standalone: true,
  template: `<div #container class="w-full h-full"></div>`,
  styles: [`:host { display: block; width: 100%; height: 100%; overflow: hidden; }`],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class WulffNet implements AfterViewInit, OnDestroy, OnChanges {
  @ViewChild('container') containerRef!: ElementRef<HTMLDivElement>;

  @Input() azimuth = 0;
  @Input() polar = 0;
  @Input() isSphereMode = false;
  @Input() uvw = { u: 0, v: 0, w: 1 };

  @Output() azimuthChange = new EventEmitter<number>();
  @Output() polarChange = new EventEmitter<number>();
  @Output() uvwChange = new EventEmitter<{ u: number, v: number, w: number }>();

  private scene!: THREE.Scene;
  private camera!: THREE.PerspectiveCamera;
  private renderer!: THREE.WebGLRenderer;
  private controls!: OrbitControls;
  private animationId: number | null = null;

  private netGroup = new THREE.Group();
  private axesGroup = new THREE.Group();
  private sphereWireframe!: THREE.LineSegments;
  private upperMarker!: THREE.Mesh;
  private uvwVector?: THREE.ArrowHelper;

  private isInternalUpdate = false;

  // --- КРИСТАЛЛОГРАФИЧЕСКАЯ ЛОГИКА ---

  private getGCD(a: number, b: number): number {
    return b === 0 ? Math.abs(a) : this.getGCD(b, a % b);
  }

  private simplifyIndices(u: number, v: number, w: number) {
    const factor = 1000; 
    const U = Math.round(u * factor);
    const V = Math.round(v * factor);
    const W = Math.round(w * factor);
    const common = this.getGCD(U, this.getGCD(V, W)) || 1;
    return { u: U / common, v: V / common, w: W / common };
  }

  ngOnChanges(changes: SimpleChanges) {
    if (!this.scene || this.isInternalUpdate) return;
    if (changes['uvw'] && !changes['azimuth'] && !changes['polar']) {
      this.syncAnglesFromUVW();
    } else if (changes['azimuth'] || changes['polar']) {
      this.syncUVWFromAngles();
    }
    this.updateView();
  }

  private syncAnglesFromUVW() {
    const v = new THREE.Vector3(this.uvw.u, -this.uvw.v, this.uvw.w);
    if (v.length() > 0) {
      v.normalize();
      this.azimuthChange.emit(Math.round(((-Math.atan2(v.y, v.x) * 180 / Math.PI) + 360) % 360));
      this.polarChange.emit(Math.round(Math.acos(v.z) * (180 / Math.PI)));
    }
  }

  private syncUVWFromAngles() {
    const phi = -(this.azimuth) * (Math.PI / 180);
    const theta = (this.polar) * (Math.PI / 180);
    const u = Math.sin(theta) * Math.cos(phi);
    const v = -(Math.sin(theta) * Math.sin(phi));
    const w = Math.cos(theta);
    this.uvwChange.emit(this.simplifyIndices(u, v, w));
  }

  // --- ИНИЦИАЛИЗАЦИЯ ---

  ngAfterViewInit() {
    this.initThree();
    this.createObjects();
    this.updateView();
    this.animate();
  }

  private initThree() {
    const container = this.containerRef.nativeElement;
    this.scene = new THREE.Scene();
    this.scene.background = new THREE.Color(0x0f172a);
    this.camera = new THREE.PerspectiveCamera(40, container.clientWidth / container.clientHeight, 0.1, 1000);
    this.camera.position.set(0, 0, 4.5);

    this.renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true });
    this.renderer.setPixelRatio(window.devicePixelRatio);
    this.renderer.setSize(container.clientWidth, container.clientHeight);
    container.appendChild(this.renderer.domElement);

    this.controls = new OrbitControls(this.camera, this.renderer.domElement);
    this.controls.enableDamping = true;
    this.controls.dampingFactor = 0.05;

    window.addEventListener('resize', () => {
      this.camera.aspect = container.clientWidth / container.clientHeight;
      this.camera.updateProjectionMatrix();
      this.renderer.setSize(container.clientWidth, container.clientHeight);
    });
  }

  private createObjects() {
    // --- ГЕНЕРАЦИЯ СЕТКИ ВУЛЬФА (2D, 2 ГРАДУСА) ---
    const step = 2;
    const proj = (lon: number, lat: number) => {
      const x = Math.cos(lat) * Math.sin(lon);
      const y = Math.sin(lat);
      const z = Math.cos(lat) * Math.cos(lon);
      const f = 1 / (1 + Math.max(z, -0.999));
      return new THREE.Vector3(x * f, y * f, 0);
    };

    const addLine = (points: THREE.Vector3[], isMajor: boolean) => {
      const geometry = new THREE.BufferGeometry().setFromPoints(points);
      const material = new THREE.LineBasicMaterial({ 
        color: 0x94a3b8, transparent: true, opacity: isMajor ? 0.4 : 0.12 
      });
      this.netGroup.add(new THREE.Line(geometry, material));
    };

    for (let i = -90; i <= 90; i += step) {
      const ptsV: THREE.Vector3[] = [], ptsH: THREE.Vector3[] = [];
      for (let j = -90; j <= 90; j += 2) {
        ptsV.push(proj((i * Math.PI) / 180, (j * Math.PI) / 180));
        ptsH.push(proj((j * Math.PI) / 180, (i * Math.PI) / 180));
      }
      addLine(ptsV, i % 10 === 0);
      addLine(ptsH, i % 10 === 0);
    }
    this.scene.add(this.netGroup);

    // --- ОСИ (A - вниз, B - вправо, C - на нас) ---
    const addAxis = (dir: THREE.Vector3, color: number, label: string) => {
      const arrow = new THREE.ArrowHelper(dir.clone().normalize(), new THREE.Vector3(0,0,0), 1.3, color, 0.1, 0.05);
      this.axesGroup.add(arrow);
      const sprite = this.createLabel(label, color);
      sprite.position.copy(dir.clone().multiplyScalar(1.5));
      this.axesGroup.add(sprite);
    };
    addAxis(new THREE.Vector3(0, -1, 0), 0xff4444, 'A'); 
    addAxis(new THREE.Vector3(1, 0, 0), 0x3b82f6, 'B'); 
    addAxis(new THREE.Vector3(0, 0, 1), 0x22c55e, 'C'); 
    this.scene.add(this.axesGroup);

    // --- ЯРКАЯ ПРОВОЛОЧНАЯ СФЕРА (3D) С ПОЛЮСАМИ НА ОСИ C ---
    const sphereGeom = new THREE.SphereGeometry(1, 36, 18);
    // Поворачиваем геометрию на 90 градусов, чтобы полюса совпали с осью Z (C)
    sphereGeom.rotateX(Math.PI / 2); 

    const edges = new THREE.EdgesGeometry(sphereGeom);
    this.sphereWireframe = new THREE.LineSegments(
      edges,
      new THREE.LineBasicMaterial({ 
        color: 0xf8fafc, 
        transparent: true,
        opacity: 0.3 // Яркая сетка
      })
    );
    this.scene.add(this.sphereWireframe);

    // Маркер точки
    this.upperMarker = new THREE.Mesh(
      new THREE.SphereGeometry(0.01, 32, 32), 
      new THREE.MeshBasicMaterial({ color: 0xffffff })
    );
    this.scene.add(this.upperMarker);

    this.scene.add(new THREE.AmbientLight(0xffffff, 1.0));
  }

  private createLabel(text: string, color: number): THREE.Sprite {
    const canvas = document.createElement('canvas');
    const ctx = canvas.getContext('2d')!;
    canvas.width = 128; canvas.height = 128;
    ctx.fillStyle = `#${color.toString(16).padStart(6, '0')}`;
    ctx.font = 'bold 80px Inter, Arial'; ctx.textAlign = 'center'; ctx.textBaseline = 'middle';
    ctx.fillText(text, 64, 64);
    const sprite = new THREE.Sprite(new THREE.SpriteMaterial({ map: new THREE.CanvasTexture(canvas), depthTest: false }));
    sprite.scale.set(0.25, 0.25, 1);
    return sprite;
  }

  private updateView() {
    this.isInternalUpdate = true;
    this.netGroup.visible = !this.isSphereMode;
    this.sphereWireframe.visible = this.isSphereMode;

    const dir = new THREE.Vector3(this.uvw.u, -this.uvw.v, this.uvw.w);
    if (dir.length() > 0) {
      const norm = dir.clone().normalize();
      if (this.isSphereMode) {
        this.upperMarker.position.copy(norm);
      } else {
        const f = 1 / (1 + Math.abs(norm.z));
        this.upperMarker.position.set(norm.x * f, norm.y * f, 0.001);
        (this.upperMarker.material as THREE.MeshBasicMaterial).color.set(norm.z < 0 ? 0x94a3b8 : 0xffffff);
      }
      if (this.uvwVector) this.scene.remove(this.uvwVector);
      this.uvwVector = new THREE.ArrowHelper(norm, new THREE.Vector3(0,0,0), 1.15, 0xfacc15, 0.1, 0.05);
      this.scene.add(this.uvwVector);
    }
    this.isInternalUpdate = false;
  }

  private animate() {
    this.animationId = requestAnimationFrame(() => this.animate());
    this.controls.update();
    this.renderer.render(this.scene, this.camera);
  }

  ngOnDestroy() {
    if (this.animationId) cancelAnimationFrame(this.animationId);
    this.renderer.dispose();
    this.scene.clear();
  }
}