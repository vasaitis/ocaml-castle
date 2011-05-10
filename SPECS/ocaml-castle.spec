Summary: OCaml bindings for libcastle
Name: ocaml-castle
Version:        %{buildver}
Release:        %{buildrev}
License: MIT
Group: Filesystem
Source0: %{name}-%{version}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root

Provides:       %{name}-%{changesetver}

BuildRequires: libcastle-devel
BuildRequires: ocaml-utils
BuildRequires: ocaml-findlib

%define _use_internal_dependency_generator 0
%define __find_requires /usr/lib/rpm/ocaml-find-requires.sh
%define __find_provides /usr/lib/rpm/ocaml-find-provides.sh

%description
OCaml Castle package

%prep
%setup -q -n %{name}

%build
make all

%install
rm -rf %{buildroot}
export DESTDIR=%{buildroot}
export OCAMLFIND_DESTDIR=%{buildroot}%{_libdir}/ocaml
export OCAMLFIND_LDCONF=ignore
mkdir -p $OCAMLFIND_DESTDIR
mkdir -p %{buildroot}/usr/bin
make install BUILD_ROOT=%{buildroot}

%clean
rm -rf %{buildroot}

%files
%defattr(-,root,root,-)
%{_libdir}/ocaml/castle/

%changelog
* Wed Sep  8 2010 Andrew Suffield <asuffield@acunu.com> - %{buildver}-%{buildrev}
- Initial package
